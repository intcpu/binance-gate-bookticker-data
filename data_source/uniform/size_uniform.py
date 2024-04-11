from common import c_enum as pb


def read_pair_info_file():
    import pandas as pd
    results = {}
    platform_list = [pb.Exchange.BINANCE.value, pb.Exchange.GATE.value]
    market_list = [pb.MarginType.USDTFUTURE.value]
    for p in platform_list:
        for m in market_list:
            fn = f'./data_file/pair_info_{p}_{m}.csv'
            df = pd.read_csv(fn).to_dict(orient='records')
            for d in df:
                results[f"{d['pairId']}_{p}_{m}"] = d
    return results


pair_info = read_pair_info_file()
