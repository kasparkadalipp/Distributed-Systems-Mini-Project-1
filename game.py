import re
from enum import Enum


class Marker(Enum):
    X = 'x'
    O = 'o'
    Empty = '_'


WINNING_PATTERNS = (
    "???......",
    "...???...",
    "......???",
    "?..?..?..",
    ".?..?..?.",
    "..?..?..?",
    "..?.?.?..",
    "?...?...?",
)


class Game:
    def __init__(self):
        self.board = Marker.Empty.value * 9

    def checkWinner(self):
        for pattern in WINNING_PATTERNS:
            for mark in [Marker.X.value, Marker.O.value]:
                if re.match(pattern.replace("?", mark), self.board):
                    return mark

    def validateMove(self, position, marker: Marker):
        count_x = self.board.count(Marker.X.value)
        count_o = self.board.count(Marker.O.value)
        if position < 0 or position > 9:
            return False
        if marker.value == Marker.X.value and count_x - count_o == 1:
            return False
        if marker.value == Marker.O.value and count_o - count_x == 1:
            return False
        if self.board[position] != Marker.Empty.value:
            return False
        return True

    def placeMarker(self, position, marker: Marker):
        board = self.board
        self.board = board[:position] + marker.value + board[position + 1:]

    def resetBoard(self):
        self.board = Marker.Empty.value * 9
