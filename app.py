import threading
import subprocess
from tictactoenode import Node
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
    def __init__(self, stop_event, port, etcd_host, etcd_port):
        threading.Thread.__init__(self)
        self.daemon = True
        self.stop_event = stop_event
        self.etcd_host = etcd_host
        self.etcd_port = etcd_port
        self.port = port


    def run(self):
        node = Node(node_port=self.port, etcd_host=self.etcd_host,
                    etcd_port=self.etcd_port)

        while not self.stop_event.is_set():
            time.sleep(3)


def start_node_threads(num_nodes, etcd_host, etcd_port):
    node_threads = []
    for i in range(num_nodes):
        node_thread = NodeThread(
            threading.Event(), 50000+i, etcd_host, etcd_port)
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
