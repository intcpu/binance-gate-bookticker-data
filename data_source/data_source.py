import importlib
from common import c_enum as pb


class SourceError(Exception):
    pass


class DataSource(object):

    def __init__(self):
        pass

    async def subscribe_book_ticker(self, platform: pb.Exchange, market: pb.MarginType, pair: (str, list), **kwargs):
        if type(pair) != list:
            pair = [pair]
        module_name = f"data_source.websocket.{platform.value}.{market.value}"
        module = importlib.import_module(module_name)
        func_name = f"subscribe_{platform.value}_{market.value}_bookticker_ws"
        func = getattr(module, func_name)

        stream = func(pair, **kwargs)
        return stream
