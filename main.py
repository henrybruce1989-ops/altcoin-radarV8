# =====================================================
# Altcoin Pump & Dump Radar (3m Version)
# =====================================================

import os
import csv
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
from binance.client import Client
from binance import ThreadedWebsocketManager
import threading
import requests

# =====================================================
# 北京时间
# =====================================================

BEIJING_TZ = timezone(timedelta(hours=8))

def bj_time():
    return datetime.now(BEIJING_TZ)

# =====================================================
# 配置
# =====================================================

API_KEY = os.getenv("API_KEY", "")
API_SECRET = os.getenv("API_SECRET", "")
SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY", "sctp14659thuntd89pzhhlsmbwynooxu")

VOLUME_THRESHOLD_24H = 15000000
SIGNAL_CSV = "signals_3m.csv"
SCAN_INTERVAL = 10

# =====================================================
# Binance Client
# =====================================================

client = Client(API_KEY, API_SECRET)

# =====================================================
# Server酱
# =====================================================

def send_server_chan(title, content):

    url = f"https://sctapi.ftqq.com/{SERVER_CHAN_KEY}.send"

    data = {
        "title": title,
        "desp": content
    }

    try:
        requests.post(url, data=data, timeout=5)
    except Exception as e:
        print("推送失败:", e)

# =====================================================
# CSV
# =====================================================

def save_csv(data):

    file_exists = os.path.isfile(SIGNAL_CSV)

    with open(SIGNAL_CSV,"a",newline='',encoding="utf-8-sig") as f:

        writer = csv.writer(f)

        if not file_exists:

            writer.writerow([
                "time",
                "symbol",
                "direction",
                "score",
                "pct",
                "volume_ratio",
                "velocity",
                "momentum",
                "consec_score"
            ])

        writer.writerow(data)

# =====================================================
# 获取交易对
# =====================================================

def get_filtered_symbols():

    tickers = client.futures_ticker()

    symbols = [
        t["symbol"]
        for t in tickers
        if float(t["quoteVolume"]) >= VOLUME_THRESHOLD_24H
        and t["symbol"].endswith("USDT")
    ]

    return symbols

# =====================================================
# 连续爆发缓存
# =====================================================

consec_cache = {}

# =====================================================
# 评分系统
# =====================================================

def score_radar(df, symbol):

    last = df.iloc[-1]

    pct = (last["close"] - last["open"]) / last["open"] * 100

    direction = "UP" if pct > 0 else "DOWN"

    pct_abs = abs(pct)

    # ===== 3分钟K线必须 >=3% 才参与评分 =====
    if pct_abs < 3:
        return None

    window = min(20, len(df))

    ma_vol = df["volume"].rolling(window).mean().iloc[-1]

    if pd.isna(ma_vol) or ma_vol == 0:
        return None

    vol_ratio = last["volume"] / ma_vol

    if vol_ratio < 1.5:
        return None

    score = 0

    # ===== 单K幅度 =====
    if pct_abs >= 6:
        score += 4
    elif pct_abs >= 5:
        score += 3
    elif pct_abs >= 4:
        score += 2
    else:
        score += 1

    # ===== 成交量 =====
    if vol_ratio >= 3:
        score += 3
    elif vol_ratio >= 2:
        score += 2
    else:
        score += 1

    # ===== 速度 =====
    if len(df) >= 4:

        velocity = (
            df["close"].iloc[-1] -
            df["open"].iloc[-4]
        ) / df["open"].iloc[-4] * 100

    else:

        velocity = 0

    velocity_abs = abs(velocity)

    if velocity_abs >= 8:
        score += 2
    elif velocity_abs >= 5:
        score += 1

    # ===== 动量 =====
    ema20 = df["close"].ewm(span=20, min_periods=1).mean().iloc[-1]

    if direction == "UP":
        momentum = last["close"] > ema20
    else:
        momentum = last["close"] < ema20

    if momentum:
        score += 1

    # ===== 连续爆发 =====
    pct_list = consec_cache.get(symbol, [])

    pct_list.append(pct)

    if len(pct_list) > 3:
        pct_list.pop(0)

    consec_cache[symbol] = pct_list

    consec_score = 0

    if len(pct_list) >= 2:

        if direction == "UP":
            count = sum(1 for p in pct_list if p >= 3)
        else:
            count = sum(1 for p in pct_list if p <= -3)

        if count >= 2:
            consec_score = 1

        if count >= 3:
            consec_score = 2

        if count == 3 and pct_abs >= 6:
            consec_score = 3

    total_score = score + consec_score

    return total_score,pct,vol_ratio,velocity,momentum,consec_score,direction

# =====================================================
# 信号等级
# =====================================================

def signal_level(score):

    if score >= 10:
        return "🚀绝佳"
    elif score >= 7:
        return "🔥优质"
    else:
        return "⚡普通"

# =====================================================
# WebSocket
# =====================================================

processed = set()

lock = threading.Lock()

def handle_kline(msg):

    try:

        symbol = msg['s']
        k = msg['k']

        if not k['x']:
            return

        hist = client.futures_klines(symbol=symbol, interval='3m', limit=30)

        df = pd.DataFrame(hist)

        df = df.iloc[:,0:6]

        df.columns = [
            'time',
            'open',
            'high',
            'low',
            'close',
            'volume'
        ]

        df[['open','high','low','close','volume']] = df[['open','high','low','close','volume']].astype(float)

        result = score_radar(df, symbol)

        if result is None:
            return

        score,pct,vol_ratio,velocity,momentum,consec_score,direction = result

        level = signal_level(score)

        icon = "📈涨" if direction == "UP" else "📉跌"

        with lock:

            if symbol in processed:
                return

            processed.add(symbol)

        msg_send = f"""
币对: {symbol}

信号: {icon}
信号等级: {level} ({score}/13)

3m单K涨跌: {pct:.2f}%
成交量倍率: {vol_ratio:.2f}x
速度: {velocity:.2f}%
动量: {"强" if momentum else "弱"}
连续爆发评分: {consec_score}

时间: {bj_time().strftime('%Y-%m-%d %H:%M:%S')} GMT+8
"""

        send_server_chan(
            f"{icon} {symbol} {level}",
            msg_send
        )

        save_csv([
            bj_time(),
            symbol,
            direction,
            score,
            pct,
            vol_ratio,
            velocity,
            momentum,
            consec_score
        ])

    except Exception as e:

        print("K线处理异常:", e)

# =====================================================
# 心跳
# =====================================================

def heartbeat():

    while True:

        try:

            symbols = get_filtered_symbols()

            print(
                f"[{bj_time().strftime('%H:%M:%S')}] "
                f"扫描:{len(symbols)} "
                f"推送:{len(processed)}"
            )

        except Exception as e:

            print("心跳异常:", e)

        time.sleep(SCAN_INTERVAL)

# =====================================================
# 主程序
# =====================================================

def main():

    symbols = get_filtered_symbols()

    print("启动交易对数量:", len(symbols))

    twm = ThreadedWebsocketManager(
        api_key=API_KEY,
        api_secret=API_SECRET
    )

    twm.start()

    for s in symbols:

        twm.start_kline_socket(
            callback=handle_kline,
            symbol=s,
            interval='3m'
        )

    threading.Thread(
        target=heartbeat,
        daemon=True
    ).start()

    while True:
        time.sleep(1)

# =====================================================

if __name__ == "__main__":
    main()