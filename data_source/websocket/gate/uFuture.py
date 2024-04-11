import json
import time

from data_source.websocket.connection import subscribe
from data_source.uniform.size_uniform import pair_info

ufuture_url = "wss://fx-ws.gateio.ws/v4/ws/usdt"


async def subscribe_gate_uFuture_bookticker_ws(symbols: list, **kwargs):
    """
    订阅gate ufuture bookticker
    :param symbols: ["BTC_USDT",]
    :return:
    """
    # symbol_lower = [i.replace("_", "").lower() for i in symbols]
    params = [{"time": int(time.time()),
               "channel": "futures.book_ticker",
               "event": "subscribe",  # "unsubscribe" for unsubscription
               "payload": symbols}]

    # 获取数据
    async for message in subscribe(ufuture_url, params):
        message = json.loads(message)
        # 过滤推送信息
        if message['event'] == 'update':
            # 获取交易对名称以及合约乘数信息
            pair = message['result']['s']
            mul = pair_info[f"{pair}_gate_uFuture"]['mul']

            # 根据api service的返回接口，转换结果
            message = {
                'base': {
                    'platform': 'gate',
                    'uid': 'uFutures',
                    'type': 'SUBSCRIBENOTIFY'
                },
                'bookTicker': {
                    'pairId': pair,
                    'updateId': message['result']['u'],
                    'eventTime': message['result']['t'],
                    'optimalBidPrice': float(message['result']['b']),
                    'optimalBidSize': float(message['result']['B']) * mul,
                    'optimalAskPrice': float(message['result']['a']),
                    'optimalAskSize': float(message['result']['A']) * mul
                }
            }
        yield message