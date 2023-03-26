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
import datetime


# Hackish way to get the address to bind to.
# Error prone, as computers tend to have multiple interface,
# but good enough for this project...
def get_host_ip():
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)

class Game:

    def __init__(self):
        self.board = [" " for _ in range(9)]
        self.board_map = {}
        self.turn = 0
        self.winner = None
        self.winning_combination = None
        self.players = ["O", "X"]
        self.move_list = {}

    def get_board(self):

        return f""" 
        
        
                    {self.board[0]}|{self.board[1]}|{self.board[2]}
                    -----
                    {self.board[3]}|{self.board[4]}|{self.board[5]}
                    -----
                    {self.board[6]}|{self.board[7]}|{self.board[8]}
                    
                    
                    
            """

    def move(self, player, position):
        symbol = "O" if player == 1 else "X"
        print(f"Player {symbol} is moving to position {position}")
        if self.board[position] == " ":
            self.board[position] = symbol
            self.move_list[position] = (symbol, datetime.datetime.now())
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

class Node(protocol_pb2_grpc.GameServiceServicer):
    def __init__(self, node_port, etcd_host, etcd_port):
        self.server = None
        self.timeout = 1  # timeout used for RPC calls in seconds
        self.leader_id = None
        self.node_id = node_port  # self.generate_unique_node_id()
        self.port = node_port
        self.address = f"{get_host_ip()}:{self.port}"
        self.leader_address = None
        self.etcd = etcd3.client(host=etcd_host, port=etcd_port)
        # =====
        self.game = Game()
        self.game_over = False
        self.leader = False
        self.player = None
        self.players = {}  # <playerid, (address,symbol)>, players[game_id] = {} would be another way to manage concurrently
        self.is_turn = False
        self.symbol = None
        self.winner_id = None
        self.waiting_for_move = None
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
            user_input = input("Enter a command: ").replace(' ', '').lower()
            if match := re.match('^set-symbol([0-9]),([xo])$', user_input):
                position, marker = match.groups()
                self.send_move(int(position), marker)
            elif re.match('^list-board$', user_input):
                self.list_board()
            elif re.match('^join$', user_input):
                self.join_game()
            elif match := re.match('^set-node-time' + 'node-\d+' + '(\d\d):(\d\d):(\d\d)$', user_input):
                hh, mm, ss = match.groups()
                # TODO
            elif match := re.match('^set-time-out' + 'players(\d+)$', user_input):
                timout = match.group(1)
                # TODO
            elif match := re.match('^set-time-out' + 'game-master(\d+)$', user_input):
                timout = match.group(1)
                # TODO
            elif match := re.match('^debug$', user_input):
                import pprint as pp
                pp.pprint(self.__dict__)
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

    def join_game(self):
        """Requests to join game"""
        with grpc.insecure_channel(self.leader_address) as channel:
            stub = protocol_pb2_grpc.GameServiceStub(channel)
            request = protocol_pb2.JoinGameRequest(request_id=self.node_id)
            response = stub.JoinGame(request, timeout=self.timeout)
            if response.status != 1:
                return False
            self.symbol = "O" if response.marker == 1 else "X"
            print(f"Joined game as {self.symbol} player")
            return True

    def JoinGame(self, request, context):

        symbol = None
        cluster = None
        # leader cannot join game
        if request.request_id == self.leader_id:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Leader cannot join game")

        if request.request_id not in self.players:
            cluster = dict(self.cluster_nodes())
            symbol = "X" if len(self.players) == 1 else "O"
            self.players[request.request_id] = (cluster[request.request_id], symbol)

        elif self.players[request.request_id]:
            context.abort(grpc.StatusCode.ALREADY_EXISTS, f"Player already in game: {self.players}")

        print(f"Player joined: {self.players} (symbol: {symbol}), address: {cluster[request.request_id]}")

        return protocol_pb2.JoinGameResponse(status=1, marker=symbol)

    def request_player_to_move(self, player_id):
        """Requests player to make a move"""
        player_address, symbol = self.players[player_id]
        with grpc.insecure_channel(player_address) as channel:
            stub = protocol_pb2_grpc.GameServiceStub(channel)
            response = stub.PlayerTurn(
                protocol_pb2.PlayerTurnRequest(request_id=self.node_id, board_state=str(self.game.get_board()),
                                               marker=symbol), timeout=self.timeout)
            if response.status == 1:
                print(response.message)
                self.waiting_for_move = True
                return True

    def PlayerTurn(self, request, context):
        """Sets player turn to be able to place marker"""
        # only leader can set player turn
        if request.request_id != self.leader_id:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Only leader can set player turn")
        symbol = "O" if request.marker == 1 else "X"
        # print(f"Player turn {symbol}.")
        self.is_turn = True

        return protocol_pb2.PlayerTurnResponse(status=1, message=f"Player {symbol} turn set; {str(request.board_state)}")

    def ListBoard(self, request, context):
        """Lists the board"""
        # game must be started
        return protocol_pb2.ListBoardResponse(status=1, message=f"Board: {self.game.get_board()}",
                                              board=self.game.get_board())

    def list_board(self):
        """Lists the board"""
        with grpc.insecure_channel(self.leader_address) as channel:
            stub = protocol_pb2_grpc.GameServiceStub(channel)
            response = stub.ListBoard(protocol_pb2.ListBoardRequest(), timeout=self.timeout)
            if response.status != 1:
                return None
            print(response.message)
            return response.board

    def send_move(self, board_position, marker):
        """Sends move to leader"""
        leader_address = self.leader_address
        if marker.upper() != self.symbol:
            print("Wrong marker")
        with grpc.insecure_channel(leader_address) as channel:
            stub = protocol_pb2_grpc.GameServiceStub(channel)
            response = stub.PlaceMarker(
                protocol_pb2.PlaceMarkerRequest(request_id=self.node_id, board_position=board_position,
                                                marker=self.symbol), timeout=self.timeout)
            if response.status == 1:
                self.is_turn = False
                print(response.message)
                return True

    def declare_winner(self):
        """Announces winner to all players"""
        self.winner_id = [player_id for player_id, player in self.players.items() if player[1] == self.game.winner][0]
        for player_id, player in self.players.items():
            player_address, symbol = player
            with grpc.insecure_channel(player_address) as channel:
                stub = protocol_pb2_grpc.GameServiceStub(channel)
                stub.DeclareWinner(protocol_pb2.DeclareWinnerRequest(winner_id=self.winner_id), timeout=self.timeout)

    def DeclareWinner(self, request, context):
        """Announces winner"""
        print(f"Game over. Winner: {request.winner_id}")
        if request.winner_id == self.node_id:
            print("=============WINNER!==============")
        return protocol_pb2.Acknowledged()

    def PlaceMarker(self, request, context):
        if self.game_over:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Game over")
        if not self.game.move(request.marker, request.board_position):
            return context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid move")
        self.game_over = self.game.winner is not None
        self.waiting_for_move = False
        return (
            protocol_pb2.PlaceMarkerResponse(
                status=1, message=f"Player {self.player} won!"
            )
            if self.game_over
            else protocol_pb2.PlaceMarkerResponse(
                status=1,
                message=f"Player {self.player} placed marker at {request.board_position}, board: {self.game.get_board()}",
            )
        )


def main_leader(node):
    """Main function for leader"""
    # wait for 2 players to join
    while len(node.players) < 2:
        time.sleep(1)
    print("Game started")
    while not node.game_over:
        while not node.waiting_for_move:
            print("Waiting for move")
            player_symbol = node.game.get_player_turn()
            player_id = [player_id for player_id, player in node.players.items() if player[1] == player_symbol][0]
            print(f"Sending request to player {player_id}, {player_symbol}")
            node.request_player_to_move(player_id)
            time.sleep(10)

    # announce winner
    node.declare_winner()
    print("Game over")


def main_client(node):
    """Main function for client"""
    while True:
        node.accept_user_input()


def main():
    node_port = int(sys.argv[1])
    etcd_host = 'localhost'
    etcd_port = 2379

    etcd_host, etcd_port = "localhost", 2379

    node = Node(node_port, etcd_host, etcd_port)

    time.sleep(1)

    # start leader thread
    if node.leader_id == node.node_id:
        print("initiating leader thread")
        leader_thread = threading.Thread(target=main_leader, args=(node,))
        leader_thread.start()
    else:
        print("initiating client thread")
        client_thread = threading.Thread(target=main_client, args=(node,))
        client_thread.start()

    node.wait_for_termination()


if __name__ == '__main__':
    main()
