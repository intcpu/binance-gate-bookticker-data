import time

import asyncio
import traceback
from datetime import datetime

import pandas as pd

from common import c_enum as pb
from config_define import stt_bt as cpub
from func_lib import common
from func_lib.func_set_var import *
from data_source.uniform.size_uniform import pair_info

import csv
from collections import deque


class bt_queue():
    def __init__(self, size, pair):
        self.size = size
        self.pair = pair
        self.queue = deque(maxlen=size)
        self.okx_queue = deque(maxlen=size)
        self.bn_queue = deque(maxlen=size)
        self.gt_queue = deque(maxlen=size)
        self.lock = asyncio.Lock()
        self.okx_lock = asyncio.Lock()
        self.bn_lock = asyncio.Lock()
        self.gt_lock = asyncio.Lock()

    async def put(self, item):
        async with self.lock:
            try:
                self.queue.append(item)
                if len(self.queue) >= self.size:
                    today = datetime.today()
                    month = today.month
                    day = today.day
                    csv_file = f'./data_bt/bt_{self.pair}_{month}_{day}.csv'
                    await self.save_to_csv(csv_file, self.queue)
                    self.queue.clear()
            except Exception as e:
                msg = f'csv error:{e}'
                vr.log.error(msg)

    async def put_bn_uf(self, item):
        async with self.bn_lock:
            try:
                self.bn_queue.append(item)
                if len(self.bn_queue) >= self.size:
                    today = datetime.today()
                    month = today.month
                    day = today.day
                    csv_file = f'./data_bt/bt_{self.pair}_{pb.Exchange.BINANCE.value}_{pb.MarginType.USDTFUTURE.value}_{month}_{day}.csv'
                    await self.save_to_csv(csv_file, self.bn_queue)
                    self.bn_queue.clear()
            except Exception as e:
                msg = f'csv error:{e}'
                vr.log.error(msg)

    async def put_gt_uf(self, item):
        async with self.gt_lock:
            try:
                self.gt_queue.append(item)
                if len(self.gt_queue) >= self.size:
                    today = datetime.today()
                    month = today.month
                    day = today.day
                    csv_file = f'./data_bt/bt_{self.pair}_{pb.Exchange.GATE.value}_{pb.MarginType.USDTFUTURE.value}_{month}_{day}.csv'
                    await self.save_to_csv(csv_file, self.gt_queue)
                    self.gt_queue.clear()
            except Exception as e:
                msg = f'csv error:{e}'
                vr.log.error(msg)

    async def save_to_csv(self, csv_file, queue):
        with open(csv_file, 'a+', newline='') as file:
            writer = csv.writer(file)
            for item in queue:
                writer.writerow(item)


async def gate_uf_bookticker(s, conf, que):
    while True:
        try:
            vr.log.info(
                f'{s} 配置地址:{id(conf)} gate永续bookticker wss启动: {pb.Exchange.GATE} {pb.MarginType.USDTFUTURE}',
                s)
            bt_val = await vr.data_source.subscribe_book_ticker(pb.Exchange.GATE,
                                                                pb.MarginType.USDTFUTURE, s)
            async for bt_info in bt_val:
                if 'is_close' in conf and conf['is_close'] == 1:
                    vr.log.warning(f'{s} 配置地址:{id(conf)} gate永续bookticker wss停止', s)
                    return
                if 'bookTicker' not in bt_info or len(bt_info) == 0:
                    continue
                # vr.log.info(bt_info, s)
                btime = int(bt_info['bookTicker']['eventTime'])
                ntime = int(time.time() * 1000)
                gask = float(bt_info['bookTicker']['optimalAskPrice'])
                gbid = float(bt_info['bookTicker']['optimalBidPrice'])
                ask_size = float(bt_info['bookTicker']['optimalAskSize'])
                bid_size = float(bt_info['bookTicker']['optimalBidSize'])
                dtype = 'gb'

                await que.put_gt_uf([ntime, btime, gask, gbid, ask_size, bid_size, dtype])


        except Exception as e:
            msg = '{} gate永续bookticker wss错误:{}'.format(s, traceback.format_exc())
            vr.log.error(msg, s)
            await asyncio.sleep(5)


async def binance_uf_bookticker(s, conf, que):
    while True:
        try:
            vr.log.info(
                f'{s} 配置地址:{id(conf)} binance永续bookticker wss启动: {pb.Exchange.BINANCE} {pb.MarginType.USDTFUTURE}',
                s)
            bt_val = await vr.data_source.subscribe_book_ticker(pb.Exchange.BINANCE,
                                                                pb.MarginType.USDTFUTURE, s)
            async for bt_info in bt_val:
                if 'is_close' in conf and conf['is_close'] == 1:
                    vr.log.warning(f'{s} 配置地址:{id(conf)} binance永续bookticker wss停止', s)
                    return
                if 'bookTicker' not in bt_info or len(bt_info) == 0:
                    continue
                # vr.log.info(bt_info, s)
                btime = int(bt_info['bookTicker']['eventTime'])
                ntime = int(time.time() * 1000)
                gask = float(bt_info['bookTicker']['optimalAskPrice'])
                gbid = float(bt_info['bookTicker']['optimalBidPrice'])
                ask_size = float(bt_info['bookTicker']['optimalAskSize'])
                bid_size = float(bt_info['bookTicker']['optimalBidSize'])
                dtype = 'bb'

                await que.put_bn_uf([ntime, btime, gask, gbid, ask_size, bid_size, dtype])

        except Exception as e:
            msg = '{} bn永续bookticker wss错误:{}'.format(s, traceback.format_exc())
            vr.log.error(msg, s)
            await asyncio.sleep(5)


async def arbi_main(stt, pair):
    try:
        pair_conf = vr.symbol_conf[pair]
        conf = pair_conf.copy()
        conf['stop'] = 0
        vr.log.warning(f'{pair} 配置:{conf} 地址:{id(conf)} 策略主协程已启动', pair)

        que = bt_queue(100, pair)
        if f'{pair}_{pb.Exchange.GATE.value}_{pb.MarginType.USDTFUTURE.value}' in pair_info:
            asyncio.create_task(gate_uf_bookticker(pair, conf, que))
        if f'{pair}_{pb.Exchange.BINANCE.value}_{pb.MarginType.USDTFUTURE.value}' in pair_info:
            # bn永续数据
            asyncio.create_task(binance_uf_bookticker(pair, conf, que))

        while True:
            try:
                conf.update(pair_conf)
                if int(time.time()) % 60 == 0:
                    vr.log.info('{}策略配置更新,地址:{},最新配置:{},全局配置:{}'.format(pair, id(conf), conf, pair_conf), pair)
                if pair_conf is None or pair_conf['enable'] == 0:
                    conf['enable'] = 0

                    if 'is_close' in conf and conf['is_close'] == 1:
                        vr.log.error(f'{pair} 配置地址:{id(conf)} 做市策略已停止:{pair not in vr.symbol_conf}', pair)
                        return None
            except Exception as e:
                msg = '{}策略协程更新配置错误:{}'.format(pair, traceback.format_exc())
                vr.log.error(msg, pair)
            await asyncio.sleep(30)
    except Exception as e:
        msg = '{}做市主程序加载错误:{}'.format(pair, traceback.format_exc())
        vr.log.error(msg, pair)


# 交易对策略及配置加载更新
async def pair_strategy_load(stt, pair):
    while True:
        try:
            if pair not in vr.run_list:
                msg = f'交易对已删除:{pair}'
                vr.log.warning(msg, pair)
                return None
            symbol_conf = vr.symbol_conf
            if pair not in symbol_conf:
                if pair in vr.pair_info:
                    vr.symbol_conf[pair] = {}
                    vr.symbol_conf[pair]['enable'] = 1
                    asyncio.create_task(arbi_main(stt, pair))
                    vr.log.info(f'交易对:{pair},三级配置初始化:{symbol_conf[pair]},id:{id(symbol_conf[pair])}', pair)
                else:
                    pair_info = vr.pair_info[pair] if pair in vr.pair_info else {}
                    vr.log.info(
                        f'{pair} 交易对未启动,pair_info:{pair_info}',
                        pair)
            else:
                infos = vr.pair_info[pair] if pair in vr.pair_info else None
                vr.log.info(f'交易对:{pair},pair_info:{infos},三级配置更新:{symbol_conf[pair]},id:{id(symbol_conf[pair])}',
                            pair)
        except Exception as e:
            msg = f'{stt} {pair}策略加载错误:{traceback.format_exc()}'
            vr.log.error(msg, pair)
        await asyncio.sleep(30)


# 创建交易对策略
async def pair_strategy_create(stt):
    vr.log.info('交易对策略创建')
    while True:
        try:
            await asyncio.sleep(1)
            run_list = vr.run_list
            pairs = []
            add_pairs = []

            # 可套利交易对
            df = pd.read_csv(cpub.pairs_file).to_dict(orient='records')
            for d in df:
                p = d['pair']
                pairs.append(p)
                if p in run_list: continue
                run_list[p] = asyncio.create_task(pair_strategy_load(stt, p), name=f'stt_load-task-{p}')
                add_pairs.append(f"{p} 已开启")
                vr.log.warning(f'{stt}策略 {p}交易对策略已创建', p)
                await asyncio.sleep(1)

            symbol_conf = vr.symbol_conf
            for p in symbol_conf:
                if p not in pairs and ('enable' not in symbol_conf[p] or symbol_conf[p]['enable'] != 0):
                    symbol_conf[p]['enable'] = 0
                    add_pairs.append(f"{p} 已关闭")
            if len(add_pairs) > 0:
                msg = f'bookticker {len(add_pairs)} 个交易对已操作\r\n' + '\r\n'.join(add_pairs)
                vr.log.info(msg)
        except Exception as e:
            msg = f'{stt} 创建策略错误:{traceback.format_exc()}'
            vr.log.error(msg)
        await asyncio.sleep(30)


# 更新交易对信息
async def update_pair_info():
    vr.log.info('更新交易对信息')
    while True:
        try:
            infos = pair_info
            new_info = vr.pair_info
            vr.ticker_info = pd.read_csv(cpub.ticker_file)
            for k, v in infos.items():
                if v['priceStep'] is None or v['minSize'] is None or v['minTurnover'] is None:
                    continue
                p = v['pairId']
                minPriceUnit = float(v['priceStep'])
                minSizeUnit = float(v['minSize'])
                price_float = common.get_float(v['priceStep'])
                size_float = common.get_float(v['minSize'])
                if p not in new_info:
                    gate_key = f"{p}_gate_uFuture"
                    if gate_key not in pair_info:
                        continue
                    mul = pair_info[gate_key]['mul']
                    new_info[p] = {}
                    new_info[p]['minPriceUnit'] = minPriceUnit
                    new_info[p]['minSizeUnit'] = minSizeUnit
                    new_info[p]['price_float'] = price_float
                    new_info[p]['size_float'] = size_float
                    new_info[p]['gate_mul'] = mul
                    new_info[p]['min_order_size'] = v['minSize']
                    new_info[p]['min_order_val'] = v['minTurnover']
                else:
                    new_info[p]['minPriceUnit'] = max(minPriceUnit, new_info[p]['minPriceUnit'])
                    new_info[p]['minSizeUnit'] = max(minSizeUnit, new_info[p]['minSizeUnit'])
                    new_info[p]['price_float'] = min(price_float, new_info[p]['price_float'])
                    new_info[p]['size_float'] = min(size_float, new_info[p]['size_float'])
                    new_info[p]['min_order_size'] = max(v['minSize'], new_info[p]['min_order_size'])
                    new_info[p]['min_order_val'] = max(v['minTurnover'], new_info[p]['min_order_val'])
        except Exception as e:
            msg = f'更新交易对信息错误:{traceback.format_exc()}'
            vr.log.error(msg)
        await asyncio.sleep(60)


async def main():
    try:
        await set_var_log(cpub.stt_name, cpub.stt_log_dir)
        await set_data_source()

        tasks = []
        tasks.append(asyncio.create_task(update_pair_info()))
        tasks.append(asyncio.create_task(pair_strategy_create(cpub.stt_name)))
        await asyncio.gather(*tasks)
    except Exception as e:
        text = f'{datetime.now()} {cpub.stt_name} 策略启动错误:{traceback.format_exc()}'
        vr.log.error(text)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        msg = f'程序被用户中断！'
        vr.log.info(msg)
    finally:
        msg = f'程序结束撤单'
        vr.log.error(msg)
