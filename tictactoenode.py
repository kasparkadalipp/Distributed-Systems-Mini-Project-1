import socket
import etcd3


def get_host_ip():
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)


class Node:

    def __init__(self, etcd_host, etcd_port, ip="", port=0, timeout_default=60, election_timeout=10,
                 election_announcement_wait_time=10):
        self.etcd_host = etcd_host
        self.etcd_port = etcd_port
        self._etcd_client = etcd3.client(host=self.etcd_host, port=self.etcd_port)
        self.node_id = self.generate_unique_node_id()
        self.ip = get_host_ip() if ip == "" else ip
        self.port = self.assign_port() if port == 0 else port
        self.address = f"{self.ip}:{self.port}"

        # not sure if i should put this here.
        self.is_leader = False
        self.stub = None
        self.timeout_default = timeout_default
        self.election_timeout = election_timeout
        self.election_announcement_wait_time = election_announcement_wait_time
        self.neighbours = {}
        self._etcd_client.put(f"/nodes/{self.node_id}", self.address)

    def generate_unique_node_id(self):
        response = self._etcd_client.get("/node_id/")
        if response[0] is None:
            print("No node id found, creating new node id")
            print("putting node id 0")
            self._etcd_client.put("/node_id/", "0")
            return 0
        else:
            response = int(response[0].decode("utf-8")) + 1
            print("putting node id", response)
            self._etcd_client.put("/node_id/", str(response))

        return response

    def assign_port(self):
        try:
            port = 50050 + self.node_id
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((self.ip, port))
            return port
        except OSError as e:
            raise RuntimeError("Couldn't find an available port") from e

    def __repr__(self):
        return f"Node({self.node_id}, {self.ip}:{self.port})"

    def discover_nodes(self):
        for node_id, address in self._etcd_client.get_prefix("/nodes/"):
            # print(node_id, address)
            address = str(node_id.decode("utf-8"))
            node_id = int(address.split(":")[-1]) - 50050
            # print(node_id)
            if node_id != self.node_id:
                self.neighbours[node_id] = address
        print(f"Discovered nodes from {self.address}: {self.neighbours}")
