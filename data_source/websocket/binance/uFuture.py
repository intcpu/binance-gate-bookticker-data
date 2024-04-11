import time

from data_source.websocket.connection import subscribe
from data_source.uniform.size_uniform import pair_info
import json

ufuture_url = "wss://fstream.binance.com/stream"

def binance_to_gate(name):
    pair_list = name.split('USDT')
    if len(pair_list) <= 1:
        raise ValueError(f"unsupported settlement currency")
    pair = pair_list[0] + '_USDT' if pair_list[1] == '' else (pair_list[0] + '_USDT' + pair_list[1].replace('_', '_20'))
    pair = pair[4:] if pair[0:4] == '1000' else pair
    return pair


async def subscribe_binance_uFuture_bookticker_ws(symbols: list, **kwargs):
    """
    订阅binance ufuture bookticker
    :param symbols: ["BTC_USDT",]
    :return:
    """
    params = [{
        "method": "SUBSCRIBE",
        "params": [f"{pair_info[f'{symbol}_binance_uFuture']['symbol'].lower()}@bookTicker" for symbol in symbols],
        # "params": ["!bookTicker"],
        "id": int(time.time() * 10 ** 6)
    }]

    # 获取数据
    async for message in subscribe(ufuture_url, params):
        message = json.loads(message)
        if 'stream' in message:
            pair = binance_to_gate(message['data']['s'])
            pmul = pair_info[f"{pair}_binance_uFuture"]['pmul']
            message = {
                'base': {
                    'platform': 'binance',
                    'uid': 'uFutures',
                    'type': 'SUBSCRIBENOTIFY'
                },
                'bookTicker': {
                    'pairId': pair,
                    'updateId': message['data']['u'],
                    'eventTime': message['data']['E'],
                    'matchTime': message['data']['T'],
                    'optimalBidPrice': float(message['data']['b']) / pmul,
                    'optimalBidSize': float(message['data']['B']) * pmul,
                    'optimalAskPrice': float(message['data']['a']) / pmul,
                    'optimalAskSize': float(message['data']['A']) * pmul
                }
            }
            yield message