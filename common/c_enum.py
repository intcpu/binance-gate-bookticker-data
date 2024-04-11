from enum import Enum


class Exchange(Enum):
    GATE = 'gate'
    BINANCE = 'binance'


class MarginType(Enum):
    USDTFUTURE = 'uFuture'
