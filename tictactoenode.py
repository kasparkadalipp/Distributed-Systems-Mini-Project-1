import socket
import sys
import random
import threading
import concurrent
import time
import grpc
import re
import etcd3
import protocol_pb2
import protocol_pb2_grpc
from concurrent import futures


# Hackish way to get the address to bind to.
# Error prone, as computers tend to have multiple interface,
# but good enough for this project...
def get_host_ip():
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)


class Node(protocol_pb2_grpc.GameServiceServicer):
    def __init__(self, node_port, etcd_host, etcd_port):
        self.server = None
        self.timeout = 1  # timeout used for RPC calls in seconds
        self.leader_id = None
        self.node_id = node_port # self.generate_unique_node_id()
        self.port = node_port
        self.address = f"{get_host_ip()}:{self.port}"
        self.leader_address = None
        self.etcd = etcd3.client(host=etcd_host, port=etcd_port)
        # =====
        self.game = Game()
        self.game_over = False
        self.leader = False
        self.player = None
        # =====
        self.serve()

        print(f"Starting node '{self.node_id}', listening on '{self.address}'")

        # Start thread for background tasks (leader election / time synchronization)
        self.daemon = threading.Thread(target=self.background_task)
        self.daemon.start()
        self.commands = threading.Thread(target=self.accept_user_input)
        self.commands.start()

    def accept_user_input(self):
        while True:
            break # TODO uncomment
            user_input = input().replace(' ', '').lower()
            if match := re.match('^set-symbol([0-9]),([xo])$', user_input):
                position, marker = match.groups()
                # TODO
            elif re.match('^list-board$', user_input):
                pass  # TODO
            elif match := re.match('^set-node-time' + 'node-\d+' + '(\d\d):(\d\d):(\d\d)$', user_input):
                hh, mm, ss = match.groups()
                # TODO
            elif match := re.match('^set-time-out' + 'players(\d+)$', user_input):
                timout = match.group(1)
                # TODO
            elif match := re.match('^set-time-out' + 'game-master(\d+)$', user_input):
                timout = match.group(1)
                # TODO
            else:
                print("Accepted commands are:\n"
                      "List-board\n"
                      "Set-symbol <position 0-9>, <marker X or O>\n"
                      "Set-node-time Node-<node-id> <hh:mm:ss>\n"
                      "Set-time-out players <time minutes>\n"
                      "Set-time-out gamer-master <time minutes>\n"
                      )
    def cluster_nodes(self):
        """Returns all nodes registered in the cluster as a tuple containing the node id and the address"""
        for address, meta in self.etcd.get_prefix("/nodes/"):
            node_id = int(meta.key.decode().split("/")[-1])
            yield node_id, address

    def background_task(self):
        while True:
            self.register()
            if not self.has_healthy_master():
                self.election()
            if self.node_id == self.leader_id:
                self.time_sync()
            time.sleep(1)

    def register(self):
        """Registers node within etcd so it is discoverable for other nodes"""
        self.etcd.put(f"/nodes/{self.node_id}", self.address)

    def has_healthy_master(self):
        """Returns true if local node is aware of current leader and current leader is up and running, i.e. reachable"""
        nodes = dict(self.cluster_nodes())
        if self.leader_id not in nodes:
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
            futures_dict = {}
            for (node_id, channel) in channels.items():
                if node_id >= self.node_id:
                    # We only need to check nodes which have potentially lower ids
                    continue
                stub = protocol_pb2_grpc.GameServiceStub(channel)
                futures_dict[node_id] = executor.submit(stub.AssumeLeader, request)
            for (node_id, future) in futures_dict.items():
                try:
                    if not future.result(timeout=self.timeout).acknowledged:
                        # Node did not not acknowledge us as leader, aborting process on this node
                        return
                except Exception:
                    # Assume node_id is dead and no longer active, remove stale entries from service discovery
                    self.etcd.delete(f"/nodes/{node_id}")

            # Either all nodes with lower node id have failed or have acknowledged us as leader, we can broadcasting us as new leader
            request = protocol_pb2.NewLeader(leader_id=self.node_id)
            _futures = []
            for channel in channels.values():
                stub = protocol_pb2_grpc.GameServiceStub(channel)
                _futures.append(executor.submit(
                    stub.NotifyOfNewLeader, request))
            concurrent.futures.wait(_futures, timeout=self.timeout)
        for channel in channels.values():
            channel.close()

    def Echo(self, request, context):
        return protocol_pb2.Pong()

    def AssumeLeader(self, request, context):
        if request.node_id > self.node_id:
            # Request originated from node with higher id, we might be able to assume leader role ourselves.
            self.election()
        return protocol_pb2.LeaderResponse(acknowledged=request.node_id < self.node_id)

    def NotifyOfNewLeader(self, request, context):
        if self.leader_id != request.leader_id:
            print(f"Elected leader: {request.leader_id}")
            self.leader_id = request.leader_id
            self.leader_address = dict(self.cluster_nodes())[self.leader_id]
        return protocol_pb2.Acknowledged()

    def time_sync(self):
        pass  # FIXME: To be implemented

    def serve(self):
        self.server = grpc.server(
            concurrent.futures.ThreadPoolExecutor(max_workers=10))
        protocol_pb2_grpc.add_GameServiceServicer_to_server(self, self.server)
        self.server.add_insecure_port(f"[::]:{self.port}")
        self.server.start()

    def wait_for_termination(self):
        self.server.wait_for_termination()

    # TODO: Check if we can somehow get an monotonically incrementing atomic(!) counter from etcd?
    # Or should we use (unsynchronized) UNIX milliseconds since epoch at startup to generate our id?
    def generate_unique_node_id(self):
        return random.randint(0, 10000)


    def players(self):
        """Gets the players in the game from etcd"""
        players_by_symbol = {}
        for value, k in self.etcd.get_prefix("/players/"):
            _, _, symbol, player_id = k.key.decode().split("/")
            players_by_symbol.setdefault(symbol, {})[player_id] = value

        return players_by_symbol



    def get_from_etcd(self, pref):
        """Gets the value from etcd"""
        d = {}
        for value, k in self.etcd.get_prefix(pref):
            keys = k.key.decode().split("/")
            for i in range(len(keys)):
                d[keys[i]] = value
        return d

    def game_started(self):
        """Checks if game has started"""
        response = self.etcd.get("/game_started")[0]
        if response is None:
            return False
        return self.etcd.get("/game_started")[0].decode() == "True"


    def JoinGame(self, request, context):

        symbol = None
        # leader cannot join game
        if request.request_id == self.leader_id:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Leader cannot join game")

        # register player in etcd
        players = self.players()

        if request.request_id in players:
            context.abort(grpc.StatusCode.ALREADY_EXISTS, f"Player already exists {players}")

        if len(players) < 2:
            symbol = "O" if players else "X"
            self.etcd.put(f"/players/{symbol}/{request.request_id}", self.address)
            players = list(self.players())

        if len(players) == 2:
            self.etcd.put("/game_started", "True")

        game_started = self.game_started()

        print(f"Player {request.request_id} joined game with symbol {symbol}, game started: {game_started}")

        return protocol_pb2.JoinGameResponse(game_started=game_started, marker=symbol)

    def PlayerTurn(self, request, context):
        """Sets player turn to be able to place marker"""
        # only leader can set player turn
        if request.request_id != self.leader_id:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Only leader can set player turn")
        # game must be started
        if not self.game_started():
            context.abort(grpc.StatusCode.UNAVAILABLE, "Game not started")
        # player turn must be valid
        symbol = request.marker
        print(f"Player turn {symbol}.")
        current_player = self.players()[symbol]
        self.player = current_player

        print(f"Player {self.player} turn")
        return protocol_pb2.Acknowledged()

    def ListBoard(self, request, context):
        """Lists the board"""
        # game must be started
        if not self.game_started():
            context.abort(grpc.StatusCode.UNAVAILABLE, "Game not started")
        return protocol_pb2.ListBoardResponse(status=1, message=f"Board: {str(self.game.board)}", board=self.game.board)

    def PlaceMarker(self, request, context):
        if not self.game_started():
            context.abort(grpc.StatusCode.UNAVAILABLE, "Game not started")
        if self.game_over:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Game over")

        players = self.players()
        print(players)
        symbol = next((k for k, v in players.items() if str(request.request_id) in v), None)

        game_turn = self.game.players[self.game.turn % 2]

        if symbol != game_turn:
            print(f"Invalid player {symbol} != {game_turn}")
            return context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid player")
        if not self.game.move(symbol, request.board_position):
            return context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid move")
        self.game_over = self.game.winner is not None
        return protocol_pb2.PlaceMarkerResponse(status="OK", message=f"Player {self.player} placed marker at {request.board_position}, board: {self.game.board}")


def main():
    node_port = int(sys.argv[1])
    etcd_host = 'localhost'
    etcd_port = 2379

    etcd_host, etcd_port = "localhost", 2379

    node = Node(node_port, etcd_host, etcd_port)
    # node.wait_for_termination()

    while True:
        try:
            # initiate_request(node, "localhost:50051", "join")
            game_started = node.game_started()
            request = input("Enter command: ")
            if node.leader_id != node.node_id and node.leader_id is not None and not game_started and request == "join":
                response = initiate_request(node, node.leader_address, request)
                node.symbol = "O" if response.marker == 1 else "X"
                print(f"Player is: {node.symbol}")
            elif node.leader_id == node.node_id and game_started and request == "turn":
                current_player = node.players()[node.game.get_player_turn()]
                print(f"Current player: {current_player}, initiating turn request")
                response = initiate_request(node, current_player, request)
                print(f"Response: {response}")
            elif game_started and request == "place":
                response = initiate_request(node, node.leader_address, request)
                print(f"Response: {response}")
            elif request == "list":
                import pprint as pp
                pp.pprint(node.__dict__)
            elif request == "board":
                print(initiate_request(node, node.leader_address, request))
            time.sleep(2)
        except Exception as e:
            print(f"Exception occurred: {e}")
            time.sleep(1)


def initiate_request(from_node, to_node_address, action, extra_args=None):
    print(f"Initiating {action} request from {from_node.node_id} to {to_node_address}")
    with grpc.insecure_channel(to_node_address) as channel:
        stub = protocol_pb2_grpc.GameServiceStub(channel)
        if action == "join":
            response = stub.JoinGame(protocol_pb2.JoinGameRequest(request_id=from_node.node_id))
        elif action == "place":
            extra_args = {}
            position = input("Enter position: ")
            extra_args["board_position"] = int(position)
            response = stub.PlaceMarker(protocol_pb2.PlaceMarkerRequest(request_id=from_node.node_id, board_position=extra_args["board_position"]))
        elif action == "turn":
            response = stub.PlayerTurn(protocol_pb2.PlayerTurnRequest(request_id=from_node.node_id, board_state=from_node.game.board, marker=from_node.game.players[from_node.game.turn % 2]))
        elif action == "board":
            response = stub.ListBoard(protocol_pb2.ListBoardRequest())
        elif action == "list":
            # print all attributes of node
            print(from_node.__dict__)
        else:
            print("Invalid action")
        return response

import datetime

class Game:

    def __init__(self):
        self.board = [" " for _ in range(9)]
        self.board_map = {}
        self.turn = 0
        self.winner = None
        self.winning_combination = None
        self.players = ["O", "X"]
        self.move_list = {}

    def convert_board(self, board):

        self.board = board

        return f""" {self.board[0]}|{self.board[1]}|{self.board[2]}
                    -----
                    {self.board[3]}|{self.board[4]}|{self.board[5]}
                    -----
                    {self.board[6]}|{self.board[7]}|{self.board[8]}"""

    def move(self, player, position):

        if self.board[position] == " ":
            self.board[position] = player
            self.move_list[position] = (player, datetime.datetime.now())
            self.turn += 1
            self.check_winner()
            return True
        else:
            return False

    def check_winner(self):
        winning_combination = [
            [0, 1, 2], [3, 4, 5], [6, 7, 8],
            [0, 3, 6], [1, 4, 7], [2, 5, 8],
            [0, 4, 8], [2, 4, 6]
        ]
        for combination in winning_combination:
            if self.board[combination[0]] == self.board[combination[1]] == self.board[combination[2]] != " ":
                self.winner = self.board[combination[0]]
                self.winning_combination = combination
                return True
        if self.turn == 9:
            self.winner = "Draw"
            return True
        return False

    def get_player_turn(self):
        return self.players[self.turn % 2] if self.turn < 9 else None

    def get_board(self):
        return self.board


if __name__ == '__main__':
    main()
