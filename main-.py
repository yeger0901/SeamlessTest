import json
import pandas as pd
import numpy as np

# ---首先定义一些function---
def load_data(path="assessment_log.json"): # 把jason文件读取成python list
    print("Loading data")
    with open(path) as file:
        data = [json.loads(line) for line in file.readlines()]
    return data

def exchange_json_to_df(data): # list转换成dataframe，筛选name
    print("Loading to DataFrame")
    return pd.json_normalize( # json_normalize是python自带的function，用来标准化jason文件
        [d for d in data if d["name"] == "tc3.core.exchange"], # 指定一个name
        record_path=["app_data", "runners"],
        meta=[
            "time",
            ["app_data", "event", "id"],
            ["app_data", "status"],
            ["app_data", "marketId"],
            ["app_data", "marketName"],
        ],
        errors="ignore",
    )

def trades_json_to_df(data, selection): # list转换成dataframe，筛选name和selection ID
    print("Extracting Trades")
    trades = pd.json_normalize(
        [d for d in data if d["name"] == "tc3.execution.brokers"] # 筛选名字是tc3.execution.brokers的
        ["app_data", "instructionReports"],
        meta="time",
    )
    trades["placedDate"] = pd.to_datetime(trades["placedDate"])
    trades = trades.set_index("placedDate")
    columns = [
        "instruction.side",
        "instruction.limitOrder.price",
        "instruction.selectionId",
        "instruction.limitOrder.size",
    ]
    is_selection = trades["instruction.selectionId"] == selection
    return trades[is_selection][columns]

def extract_meta(exchange_df): # 读取指定的meta信息
    print("Extracting metadata")
    columns = ["runnerName", "selectionId", "app_data.marketId", "app_data.marketName"] # 指定列名
    return exchange_df[exchange_df["status"].isna()][columns]

def extract_bests(available, side="back"): # 一组数据中找出最好的但是默认第一组？？？？？
    try:
        s = pd.Series(available[0])
    except:
        s = pd.Series([np.NaN, np.NaN], index=["price", "size"])
    s.index = [f"{col}_{side}" for col in s.index] # 改变列的名字， 例如price改成price_back
    return s

def extract_order_book(exchange_df, market, selection):
    print("Extracting Order Book")
    is_active = exchange_df["status"] == "ACTIVE" # 筛选状态是active的
    is_selection = exchange_df["selectionId"] == selection #根据给定的selection id筛选
    is_market = exchange_df["app_data.marketId"] == market #根据给定的market id筛选
    columns = ["time", "ex.availableToBack", "ex.availableToLay"] # 筛选列

    order_book = (
        exchange_df[is_active & is_selection & is_market][columns]
        .copy()
        .set_index("time")
    )
    order_book.index = pd.to_datetime(order_book.index)
    best_backs = order_book["ex.availableToBack"].apply(extract_bests, side="back")
    best_lays = order_book["ex.availableToLay"].apply(extract_bests, side="lay")
    return pd.concat(
        [
            order_book.drop(axis=1, labels=["ex.availableToBack", "ex.availableToLay"]),
            best_backs,
            best_lays,
        ],
        axis=1,
    )



# ---开始运行function---
data = load_data() # 读取jason文件变成list
exchange_df = exchange_json_to_df(data) # list转换成dataframe，筛选名字是tc3.core.exchange的信息
# print(exchange_df)

meta = extract_meta(exchange_df) # 提取一些元信息，例如selection ID
print(meta)
market = "1.170226122"

selection_ids = meta[meta["app_data.marketId"] =="1.170226122"]["selectionId"].tolist() #根据market ID 提取 selection ID
for selection_id in selection_ids[0:5]:#
    
    print(f"extracting data for selection id {selection_id}")
    order_book = extract_order_book(exchange_df, market, selection_id) # 根据market ID和selection ID提取order book
    trades = trades_json_to_df(data, selection_id) # 根据selection ID提取trades信息
    if order_book.empty==False:
        ave_price = (order_book['price_back'] + order_book['price_lay'])/2
        totle_volumn = order_book['size_back'] + order_book['size_lay']
        trade_all = trades['instruction.limitOrder.price'] * trades['instruction.limitOrder.size']
        trades['trade_all'] = trade_all
    
        order_book_new = order_book.copy()
        order_book_new=order_book_new.reset_index()
        time_1 = order_book_new['time']
        time_1 = time_1.dt.floor('S')
        row_num, column_num = order_book_new.shape
        trade_price = np.zeros(row_num)
        
        row_num, column_num = trades.shape
        trades_new = trades.copy()
        trades_new=trades_new.reset_index()
        time_2 = trades_new['placedDate'].dt.tz_localize(None)
        trade_side = trades['instruction.side']
        idx_all = np.zeros(row_num)
        time_trade = []
        trade_size = np.zeros(row_num)
        for n in range(row_num):
            time_point = time_2[n]
            
            if trade_side[n] == 'LAY':
                trade_size_once = -trades['instruction.limitOrder.size'][n]
            else:
                trade_size_once = trades['instruction.limitOrder.size'][n]
            idx=time_1[time_1==time_point].index[0]
            while trade_price[idx] != 0:
                idx = idx+1
            trade_price[idx] = trades['instruction.limitOrder.price'][n]
            trade_size[n] = trade_size_once
            idx_all[n]=idx
            time_trade.append(order_book_new['time'][idx])
        # order_book_new['trade_price'] = trade_price
        # order_book_new['trade_size'] = trade_size
        
        fig = plt.figure()
        ax1 = fig.add_subplot(1, 1, 1)
        (markers, stemlines, baseline) = ax1.stem(order_book_new['time'],totle_volumn)
        # (markers, stemlines, baseline) = plt.stem(order_book_new['time'],totle_volumn)
        plt.setp(stemlines, linestyle="-", color="blue" )
        plt.setp(markers, visible=False)
        plt.setp(baseline, color="grey")
        
        
        if row_num>0:
            (markers, stemlines, baseline) = ax1.stem(time_trade,trade_size)
            # (markers, stemlines, baseline) = plt.stem(order_book_new['time'],totle_volumn)
            plt.setp(stemlines, linestyle="-", color="green" )
            plt.setp(markers, color="green")
            plt.setp(baseline, visible=False)
            
        # plt.plot(order_book_new['time'],order_book_new['trade'])
        ax2=ax1.twinx()
        ax2.plot(order_book_new['time'],ave_price, color='red')
        ax2.plot(time_trade,trades['instruction.limitOrder.price'],'*', color='purple')
        plt.title(str(selection_id))
        plt.show()
    

