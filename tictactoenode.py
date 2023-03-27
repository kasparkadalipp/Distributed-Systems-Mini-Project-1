import socket
import sys
import threading
import concurrent
import time
import datetime
import re
import grpc
import pprint as pp
import etcd3
from datetime import datetime
from google.protobuf.timestamp_pb2 import Timestamp
import protocol_pb2
import protocol_pb2_grpc


# Hackish way to get the address to bind to.
# Error prone, as computers tend to have multiple interface,
# but good enough for this project...
def get_host_ip():
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)


def print_board(board: str):
    print((" {} | {} | {}\n"
           "---+---+---\n"
           " {} | {} | {}\n"
           "---+---+---\n"
           " {} | {} | {}").format(*[_ for _ in board]))


class Game:

    def __init__(self, player_x, player_o):
        self.board = [" " for _ in range(9)]
        self.board_map = {}
        self.turn = 0
        self.winner = None
        self.winning_combination = None
        self.players = [player_x, player_o]
        self.markers = ["X", "O"]
        self.move_list = {}

    def get_board(self):
        return "".join(self.board)

    def get_current_marker(self):
        return self.markers[self.turn % 2]

    def get_current_player(self):
        return self.players[self.turn % 2]

    def isInvalidMove(self, position, marker, player_id):
        count_x = self.board.count("X")
        count_o = self.board.count("O")
        if position < 0 or position > 8:
            print("INVALID POSITION")
            return True
        if marker == "X" and count_x - count_o == 1:
            print("O SHOULD MOVE")
            return True
        if marker == "O" and count_o - count_x == 1:
            print("x SHOULD MOVE")
            return True
        if self.get_current_player() != player_id:
            print("PlayerID", player_id, "current player", self.get_current_player)
            print("NOT YOUR TURN")
            return True
        if self.board[position] != " ":
            print("POSITION IS NOT EMPTY")
            return True
            # marker_idx = 0 if marker == "O" else 1
            # if self.players[marker_idx] != player_id:
            #     print("NOT YOUR SYMBOL")
            # return True
        print("VALID MOVE")
        return False

    def board_is_filled(self):
        return self.board.count(" ") == 0

    def move(self, marker, position, player_id):
        if self.isInvalidMove(position, marker, player_id):
            return False

        print(f"Player {marker} is moving to position {position}")

        self.board[position] = marker
        self.move_list[position] = (marker, datetime.datetime.now())
        self.turn += 1
        self.check_winner()
        return True

    def check_winner(self):
        winning_combination = [
            [0, 1, 2], [3, 4, 5], [6, 7, 8],
            [0, 3, 6], [1, 4, 7], [2, 5, 8],
            [0, 4, 8], [2, 4, 6]
        ]
        for combination in winning_combination:  # FIXME both currently lose
            if self.board[combination[0]] == self.board[combination[1]] == self.board[combination[2]] != " ":
                player_x, player_o = self.players
                self.winner = [(player_x, "win" if player_o == self.get_current_player() else "loss"),
                               (player_o, "win" if player_x == self.get_current_player() else "loss")]
                self.winning_combination = combination
                return True
        if self.board_is_filled():
            player_x, player_o = self.players
            self.winner = [(player_x, "draw"), (player_o, "draw")]
            return True
        return False

    def get_player_turn(self):
        return self.players[self.turn % 2], self.markers[self.turn % 2]


class Node(protocol_pb2_grpc.GameServiceServicer):
    def __init__(self, node_port, etcd_host, etcd_port):
        self.server = None
        self.timeout = 10  # timeout used for RPC calls in seconds
        self.leader_id = None
        self.port = node_port
        self.address = f"{get_host_ip()}:{self.port}"
        self.leader_address = None
        self.etcd = etcd3.client(host=etcd_host, port=etcd_port)
        self.node_id = self.generate_unique_node_id()
        # =====
        self.ongoing_games = {}  # { game_id: (X-player_id, O-player_id, Game) }
        self.waiting_for_opponent = None  # Node id which is waiting for opponent
        self.symbol = None  # Maybe
        # =====
        self.serve()
        self.time_offset = 0  # offset of clock in milliseconds

        print(f"Starting node '{self.node_id}', listening on '{self.address}'")

        # Start thread for background tasks (leader election / time synchronization)
        self.daemon = threading.Thread(target=self.background_task, daemon=True)
        self.daemon.start()

    def accept_user_input(self):
        while True:
            try:
                user_input = input().replace(' ', '').lower()
                if match := re.match('^set-symbol([0-9]),([xo])$', user_input):
                    position, marker = match.groups()
                    self.send_move(int(position), marker.upper())
                elif re.match('^list-board$', user_input):
                    self.list_board()
                elif re.match('^join$', user_input):
                    self.join_game()
                elif match := re.match(r'^set-node-time' + r'node-(\d+)' + r'(\d\d):(\d\d):(\d\d)$', user_input):
                    node, hh, mm, ss = match.groups()
                    node = int(node)
                    if self.node_id != node and self.node_id != self.leader_id:
                        print(f"Rejected, non-leader node is only allowed to modify its own clock!")
                        continue
                    nodes = dict(self.cluster_nodes())
                    if not node in nodes:
                        print(f"Invalid node id {node}")
                        continue
                    with grpc.insecure_channel(nodes[node]) as channel:
                        stub = protocol_pb2_grpc.GameServiceStub(channel)
                        dt = datetime.datetime.now().replace(hour=int(hh), minute=int(hh), second=int(ss))
                        time = Timestamp()
                        time.FromDatetime(dt)
                        request = protocol_pb2.SetClockRequest(time=time)
                        print(stub.SetClock(request).message)

                elif match := re.match('^set-time-out' + 'players(\d+)$', user_input):
                    timout = match.group(1)
                    # TODO
                elif match := re.match('^set-time-out' + 'game-master(\d+)$', user_input):
                    timout = match.group(1)
                    # TODO
                elif match := re.match('^debug$', user_input):
                    pp.pprint(self.__dict__)
                else:
                    print("Accepted commands are:\n"
                          "List-board\n"
                          "Set-symbol <position 0-9>, <marker X or O>\n"
                          "Set-node-time Node-<node-id> <hh:mm:ss>\n"
                          "Set-time-out players <time minutes>\n"
                          "Set-time-out gamer-master <time minutes>\n"
                          )
            except Exception as e:
                print(e)

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
            if self.leader_id == self.node_id:
                self.time_sync()
            time.sleep(self.timeout)

    def register(self):
        """Registers node within etcd so it is discoverable for other nodes"""
        self.etcd.put(f"/nodes/{self.node_id}", self.address)

    def has_healthy_master(self):
        """Returns true if local node is aware of current leader andatetime.now()d current leader is up and running, i.e. reachable"""
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
                futures_dict[node_id] = executor.submit(
                    stub.AssumeLeader, request)
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
        return protocol_pb2.LeaderResponse(acknowledged=request.node_id <= self.node_id)

    def NotifyOfNewLeader(self, request, context):
        if self.leader_id != request.leader_id:
            print(f"Elected leader: {request.leader_id}")
            self.leader_id = request.leader_id
            self.leader_address = dict(self.cluster_nodes())[self.leader_id]
        return protocol_pb2.Acknowledged()

    def node_time(self):
        """Local node time in milliseconds since epoch"""
        return Timestamp().ToMilliseconds() + self.time_offset

    def formatted_time(self):
        return datetime.fromtimestamp(self.node_time() / 1000).strftime('%HH:%MM:%SS')

    def GetTime(self, request, context):
        cur_time = Timestamp()
        cur_time.FromMilliseconds(self.node_time())
        return protocol_pb2.TimeResponse(time=cur_time)

    def AdjustClock(self, request, context):
        self.time_offset += request.offset_ms
        return protocol_pb2.AdjustClockResponse()

    def SetClock(self, request, context):
        cur_time = Timestamp()
        self.time_offset = request.time.ToMilliseconds() - cur_time.ToMilliseconds()
        return protocol_pb2.SetClockResponse()

    def time_sync(self):
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
            avg_time = sum(times.values()) / len(times.values())
            futures = []
            for (node_id, node_time) in times.items():
                stub = protocol_pb2_grpc.GameServiceStub(channels[node_id])
                offset = int(avg_time - node_time)
                if not offset:
                    continue
                print(f"Adjusting time offset of node {node_id} by {offset} milliseconds")
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
            increment_successful = self.etcd.replace(
                '/node_counter', str(counter), str(counter + 1))
        return counter

    def join_game(self):
        """Requests to join game"""
        with grpc.insecure_channel(self.leader_address) as channel:
            stub = protocol_pb2_grpc.GameServiceStub(channel)
            request = protocol_pb2.JoinGameRequest(request_id=self.node_id)
            stub.JoinGame(request, timeout=self.timeout)

    def JoinGame(self, request, context):
        # TODO: check that player isn't already part of an ongoing game

        if request.request_id in self.ongoing_games:
            print(f"Player {request.request_id} is already part of an ongoing game")
            return protocol_pb2.JoinGameResponse(status=2)

        if self.waiting_for_opponent and self.waiting_for_opponent != request.request_id:
            game = Game(self.waiting_for_opponent, request.request_id)
            self.ongoing_games[self.waiting_for_opponent] = game
            self.ongoing_games[request.request_id] = game
            self.waiting_for_opponent = None
            player, symbol = game.get_player_turn()
            self.request_player_to_move(player, symbol)
            print("GAME STARTING")
        else:
            print(f"Adding node {request.request_id} to waiting list")
            self.waiting_for_opponent = request.request_id  # node_id

        return protocol_pb2.JoinGameResponse(status=1)

    def request_player_to_move(self, player_id, marker):
        """Requests player to make a move"""
        nodes = dict(self.cluster_nodes())

        if player_id not in nodes:
            pass
            # TODO: we're screwed
        else:
            print(f"Requesting player {player_id} to make a move {marker}")
            address = nodes[player_id]
            with grpc.insecure_channel(address) as channel:
                stub = protocol_pb2_grpc.GameServiceStub(channel)
                response = stub.PlayerTurn(
                    protocol_pb2.PlayerTurnRequest(request_id=self.node_id,
                                                   board_state=self.ongoing_games[player_id].get_board(),
                                                   marker=marker),
                    timeout=self.timeout)

    def PlayerTurn(self, request, context):
        print(f"=== {request.marker}'s turn (player)   ===")
        print_board(request.board_state)
        return protocol_pb2.PlayerTurnResponse(status=1,
                                               message=f"Player {request.marker} turn set; {request.board_state}")

    def ListBoard(self, request, context):
        """Lists the board"""
        # game must be started
        return protocol_pb2.ListBoardResponse(status=1, message=f"Board: {self.game.get_board()}",
                                              board=self.game.get_board())

    def list_board(self):
        """Lists the board"""
        with grpc.insecure_channel(self.leader_address) as channel:
            stub = protocol_pb2_grpc.GameServiceStub(channel)
            response = stub.ListBoard(
                protocol_pb2.ListBoardRequest(), timeout=self.timeout)
            if response.status != 1:
                return None
            print(response.message)
            return response.board

    def send_move(self, board_position, marker):
        """Sends move to leader"""
        leader_address = self.leader_address
        with grpc.insecure_channel(leader_address) as channel:
            stub = protocol_pb2_grpc.GameServiceStub(channel)
            response = stub.PlaceMarker(
                protocol_pb2.PlaceMarkerRequest(request_id=self.node_id, board_position=board_position,
                                                marker=marker), timeout=self.timeout)
            if response.status == 1:
                self.is_turn = False
                print(response.message)
                return True

    def announce_game_over(self, game):
        """Announces game over to all players"""
        nodes = dict(self.cluster_nodes())
        for player, result in game.winner:
            print(player, result)
            player_address = nodes[player]
            with grpc.insecure_channel(player_address) as channel:
                stub = protocol_pb2_grpc.GameServiceStub(channel)
                stub.DeclareWinner(protocol_pb2.DeclareWinnerRequest(
                    game_result=result, board_state=game.get_board()), timeout=self.timeout)

    def DeclareWinner(self, request, context):
        """Announces winner"""
        print_board(request.board_state)
        print(f"Game over: {request.game_result}")
        return protocol_pb2.Acknowledged()

    def PlaceMarker(self, request, context):
        player = request.request_id
        current_id, symbol = self.ongoing_games[request.request_id].get_player_turn()
        print(f"DEBUG: {player} {symbol}")
        # FIXME: check that the player is the one who's turn it is
        game = self.ongoing_games[request.request_id]
        if not game.move(request.marker, request.board_position, request.request_id):
            return context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid move")

        if game.winner:
            # TODO: Send final board state to both players
            self.announce_game_over(game)
            for player in game.players: self.ongoing_games.pop(player)
            return protocol_pb2.PlaceMarkerResponse(status=1, message=f"=========")
        else:
            opponent, symbol = game.get_player_turn()
            self.request_player_to_move(opponent, symbol)
            return protocol_pb2.PlaceMarkerResponse(status=1, message=f"=== {symbol}'s turn (opponent) ===")


def main():
    if len(sys.argv) == 2:
        node_port = int(sys.argv[1])
        etcd_host, etcd_port = "localhost", 2379
    elif len(sys.argv) == 3:
        node_port = int(sys.argv[1])
        etcd_host, etcd_port = sys.argv[2].split(":", 1)
        etcd_port = int(etcd_port)
    else:
        sys.exit(f"Usage: {sys.argv[0]} node-port [etcd-host:etcd-port]")

    node = Node(node_port, etcd_host, etcd_port)

    while True:
        try:
            node.accept_user_input()
        except KeyboardInterrupt:
            print("Exiting...")
            break


if __name__ == '__main__':
    main()
