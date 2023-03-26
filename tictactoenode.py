import socket
import sys
import threading
import concurrent
import time
import grpc
import etcd3
from google.protobuf.timestamp_pb2 import Timestamp
import protocol_pb2
import protocol_pb2_grpc

# Hackish way to get the address to bind to.
# Error prone, as computers tend to have multiple interface,
# but good enough for this project...
def get_host_ip():
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)


class Node(protocol_pb2_grpc.GameServiceServicer):
    def __init__(self, node_port, etcd_host, etcd_port):
        self.timeout = 10  # timeout used for RPC calls in seconds
        self.leader_id = None
        self.port = node_port
        self.address = f"{get_host_ip()}:{self.port}"
        self.etcd = etcd3.client(host=etcd_host, port=etcd_port)
        self.node_id = self.generate_unique_node_id()
        self.serve()
        self.time_offset = 0  # offset of clock in milliseconds

        print(f"Starting node '{self.node_id}', listening on '{self.address}'")

        # Start thread for background tasks (leader election / time synchronization)
        self.daemon = threading.Thread(target=self.background_task)
        self.daemon.start()

    def cluster_nodes(self):
        """Returns all nodes registered in the cluster as a tuple containing the node id and the address"""
        for address, meta in self.etcd.get_prefix("/nodes/"):
            node_id = int(meta.key.decode().split("/")[-1])
            yield (node_id, address)

    def background_task(self):
        while True:
            self.register()
            if not self.has_healthy_master():
                self.election()
            if self.node_id == self.leader_id:
                self.time_sync()
            time.sleep(self.timeout)

    def register(self):
        """Registers node within etcd so it is discoverable for other nodes"""
        self.etcd.put(f"/nodes/{self.node_id}", self.address)

    def has_healthy_master(self):
        """Returns true if local node is aware of current leader and current leader is up and running, i.e. reachable"""
        nodes = dict(self.cluster_nodes())
        if not self.leader_id in nodes:
            return False
        with grpc.insecure_channel(nodes[self.leader_id]) as channel:
            stub = protocol_pb2_grpc.GameServiceStub(channel)
            try:
                stub.Echo(protocol_pb2.Ping(), timeout=self.timeout)
                return True
            except Exception:
                return False

    def election(self):
        """initiates bullying leader election, electing node with lowest id as leader"""
        channels = {node_id: grpc.insecure_channel(address) for (
            node_id, address) in self.cluster_nodes()}
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(channels)) as executor:
            request = protocol_pb2.LeaderRequest(node_id=self.node_id)
            futures = {}
            for (node_id, channel) in channels.items():
                if node_id >= self.node_id:
                    # We only need to check nodes which have potentially lower ids
                    continue
                stub = protocol_pb2_grpc.GameServiceStub(channel)
                futures[node_id] = executor.submit(stub.AssumeLeader, request)
            for (node_id, future) in futures.items():
                try:
                    if not future.result(timeout=self.timeout).acknowledged:
                        # Node did not not acknowledge us as leader, aborting process on this node
                        return
                except Exception:
                    # Assume node_id is dead and no longer active, remove stale entries from service discovery
                    self.etcd.delete(f"/nodes/{node_id}")

            # Either all nodes with lower node id have failed or have acknowledged us as leader, we can broadcasting us as new leader
            request = protocol_pb2.NewLeader(leader_id=self.node_id)
            futures = []
            for channel in channels.values():
                stub = protocol_pb2_grpc.GameServiceStub(channel)
                futures.append(executor.submit(
                    stub.NotifyOfNewLeader, request))
            concurrent.futures.wait(futures, timeout=self.timeout)
        for channel in channels.values():
            channel.close()

    def Echo(self, request, context):
        return protocol_pb2.Pong()

    def AssumeLeader(self, request, context):
        if request.node_id > self.node_id:
            # Request originated from node with higher id, we might be able to assume leader role ourselves.
            self.election()
        return protocol_pb2.LeaderResponse(acknowledged=request.node_id <= self.node_id)

    def NotifyOfNewLeader(self, request, context):
        if self.leader_id != request.leader_id:
            print(f"Elected leader: {request.leader_id}")
            self.leader_id = request.leader_id
        return protocol_pb2.Acknowledged()

    def GetTime(self, request, context):
        time = Timestamp()
        time.FromMilliseconds(time.ToMilliseconds() + self.time_offset)
        return protocol_pb2.TimeResponse(time=time)

    def AdjustClock(self, request, context):
        self.time_offset += request.offset_ms
        return protocol_pb2.AdjustClockResponse()

    def SetClock(self, request, context):
        self.time_offset = request.time.ToMilliseconds() - cur_time.ToMilliseconds()
        return protocol_pb2.SetClockResponse()

    def time_sync(self):
        print("Starting time synchronization")
        channels = {node_id: grpc.insecure_channel(address) for (
            node_id, address) in self.cluster_nodes()}
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(channels)) as executor:
            request = protocol_pb2.TimeRequest()
            futures = {}
            for (node_id, channel) in channels.items():
                stub = protocol_pb2_grpc.GameServiceStub(channel)
                futures[node_id] = executor.submit(stub.GetTime, request)
            times = {}
            for (node_id, future) in futures.items():
                try:
                    times[node_id] = future.result(
                        timeout=self.timeout).time.ToMilliseconds()
                except Exception:
                    pass
            avg_time = sum(times.values())/len(times.values())
            futures = []
            for (node_id, node_time) in times.items():
                stub = protocol_pb2_grpc.GameServiceStub(channels[node_id])
                offset = int(avg_time - node_time)
                if not offset:
                    continue
                print(f"Adjusting time offset of node {node_id} by {offset}")
                request = protocol_pb2.AdjustClockRequest(offset_ms=offset)
                futures.append(executor.submit(stub.AdjustClock, request))
            concurrent.futures.wait(futures, timeout=self.timeout)
        for channel in channels.values():
            channel.close()

    def serve(self):
        self.server = grpc.server(
            concurrent.futures.ThreadPoolExecutor(max_workers=10))
        protocol_pb2_grpc.add_GameServiceServicer_to_server(self, self.server)
        self.server.add_insecure_port(f"[::]:{self.port}")
        self.server.start()

    def wait_for_termination(self):
        self.server.wait_for_termination()

    def generate_unique_node_id(self):
        # initialize counter variable if it does not already exist
        self.etcd.transaction(
            compare=[etcd3.transactions.Version('/node_counter') == 0],
            success=[etcd3.transactions.Put('/node_counter', '0')],
            failure=[]
        )
        # atomically get and increment variable
        increment_successful = False
        while not increment_successful:
            counter = int(self.etcd.get('/node_counter')[0])
            increment_successful = self.etcd.replace('/node_counter', str(counter), str(counter+1))
        return counter


def main():
    match len(sys.argv):
        case 1:
            node_port = 50000
            etcd_host, etcd_port = "localhost", 2379
        case 2:
            node_port = int(sys.argv[1])
            etcd_host, etcd_port = "localhost", 2379
        case 3:
            node_port = int(sys.argv[1])
            etcd_host, etcd_port = sys.argv[2].split(":", 1)
            etcd_port = int(etcd_port)
        case _:
            sys.exit(f"Usage: {sys.argv[0]} [node-port [etcd-host:etcd-port]]")

    node = Node(node_port, etcd_host, etcd_port)

    # FIXME: Implement endless loop emulating "console", i.e. accepting and handling user commands

    node.wait_for_termination()


if __name__ == '__main__':
    main()
