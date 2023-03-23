import threading
import subprocess
from tictactoe import Node
import etcd3
import socket
import time


class EtcdThread(threading.Thread):
    def __init__(self, stop_event):
        threading.Thread.__init__(self)
        self.daemon = True
        self.stop_event = stop_event

    def run(self):
        proc = subprocess.Popen(['etcd'])

        while not self.stop_event.is_set():
            time.sleep(1)
            break

        # clean up etcd data
        proc = subprocess.Popen(['rm', '-rf', 'default.etcd/member'])


class NodeThread(threading.Thread):
    def __init__(self, stop_event, etcd_host, etcd_port):
        threading.Thread.__init__(self)
        self.daemon = True
        self.stop_event = stop_event
        self.etcd_host = etcd_host
        self.etcd_port = etcd_port

    def run(self):
        node = Node(etcd_host=self.etcd_host, etcd_port=self.etcd_port)
        print(f"Node {node.node_id} started at {node.address}")

        while not self.stop_event.is_set():
            node.discover_nodes()
            time.sleep(3)



def start_node_threads(num_nodes, etcd_host, etcd_port):
    node_threads = []
    for _ in range(num_nodes):
        node_thread = NodeThread(threading.Event(), etcd_host, etcd_port)
        node_thread.start()
        node_threads.append(node_thread)
        time.sleep(1)
    return node_threads


def main():
    # Start etcd server
    etcd_thread = EtcdThread(threading.Event())
    etcd_thread.start()

    # Wait for server to start
    time.sleep(1)

    etcd_host = 'localhost'
    etcd_port = 2379

    # Start node threads
    node_threads = start_node_threads(3, etcd_host, etcd_port)

    # for n in node_threads:
    #     n.join()

    time.sleep(10)
    # shut down subprocess
    etcd_thread.stop_event.set()
    etcd_thread.join()


if __name__ == '__main__':
    main()



#
# def main():
#
#     nodes = [
#         Node("localhost:50051"),
#         Node("localhost:50052"),
#         Node("localhost:50053"),
#     ]
#
#     # Set the current node_id based on the command line argument or environment variable
#     node_id = int(os.environ["NODE_ID"])
#
#     server = TicTacToeServer(node_id, nodes)
#
#     # Set up and start the gRPC server
#     server_address = nodes[node_id - 1].address
#     server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
#     tictactoe_pb2_grpc.add_TicTacToeServicer_to_server(server, server)
#     server.add_insecure_port(server_address)
#     server.start()
#
#     print(f"Server started at {server_address}")
#
#     # Start the leader election process
#     server._start_leader_election()
#
#     # Keep the server running
#     try:
#         while True:
#             time.sleep(3600)
#     except KeyboardInterrupt:
#         server.stop(0)

#
# # Example usage
# if __name__ == "__main__":
#     # initialize etcd server
#     etcd_server = etcd3.server.Etcd3Server()
#     etcd_client = etcd3.client()
#     ETCD_PREFIX = "tictactoe"
#     ETCD_HOST = "localhost"
#     ETCD_PORT = 2379
#     ETCD_URL = f"http://{ETCD_HOST}:{ETCD_PORT}"
#
#     # run 3 Nodes
#     node1 = Node()
#     node2 = Node()
#     node3 = Node()
#
#     print(f"Node 1: {node1}")
#     print(f"Node 2: {node2}")
#     print(f"Node 3: {node3}")


#     client = TicTacToeClient("localhost:50051")
#     game_started = client.start_game()
#
#     if game_started:
#         success, error_message = client.set_symbol(1, "X")
#         if not success:
#             print(f"Error: {error_message}")
#
#         board_state = client.list_board()
#         print(board_state)
#
#         status = client.set_node_time("Node-2", "12:34:56")
#         print(status)
#
# if __name__ == "__main__":
#     # my_node_id = sys.argv[1]
#     # my_ip = sys.argv[2]
#     # my_port = int(sys.argv[3])
#
#     node = Node()
#
#     # Start the server
#     server = TicTacToeServer(node)
#     server.start()
#
#     time.sleep(2)  # Give the server some time to start
#
#     # Discover other nodes
#     other_nodes = node.discover_nodes()
#     del other_nodes[my_node_id]  # Remove the current node from the list
#
#     # Start the client
#     client = TicTacToeClient(node, other_nodes)
#
#     # Start the election process
#     client.start_election()
#
#     # Run the game
#     client.run_game_loop()
