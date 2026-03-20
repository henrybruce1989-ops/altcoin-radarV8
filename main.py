#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import aiohttp
import websockets
import json
import csv
import os
import time
import logging
import sys
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Dict, Set, List, Optional, Tuple
from asyncio import Queue, QueueFull

# ==================== 配置常量 ====================
REST_BASE_URL = "https://fapi.binance.com"
WEBSOCKET_BASE_URL = "wss://fstream.binance.com"

VOLUME_THRESHOLD_USDT = 8_000_000       # 24h交易额门槛（USDT）

MAX_STREAMS_PER_CONNECTION = 50
MESSAGE_QUEUE_SIZE = 200
RECONNECT_DELAY = 5
KEEPALIVE_INTERVAL = 30

SYMBOL_REFRESH_INTERVAL = 600
LOG_LEVEL = "INFO"

SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY", "sctp14659thuntd89pzhhlsmbwynooxu")
SIGNAL_COOLDOWN_SECONDS = 60            # 冷却时间（秒）

# 时间窗口聚合参数
AGGREGATION_WINDOW_SECONDS = 20          # 聚合窗口长度（秒）
VOLUME_RATIO_THRESHOLD = 2.0            # 窗口成交量是历史同期期望成交量的倍数
MIN_PRICE_CHANGE_PERCENT = 0.2          # 最小价格变化百分比（0.2%）
HISTORICAL_WINDOW_SECONDS = 300          # 历史平均成交量窗口（秒）

CSV_FILE_PATH = "signals.csv"

# 日志
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

BEIJING_TZ = timezone(timedelta(hours=8))

def get_beijing_time() -> str:
    return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")

# ==================== 币种筛选 ====================
class BinanceRestClient:
    def __init__(self):
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    async def get_24h_ticker(self) -> List[Dict]:
        url = f"{REST_BASE_URL}/fapi/v1/ticker/24hr"
        try:
            async with self.session.get(url) as resp:
                return await resp.json()
        except Exception as e:
            logger.error(f"Failed to fetch 24h ticker: {e}")
            return []

    def filter_active_symbols(self, tickers: List[Dict]) -> Set[str]:
        active = set()
        for t in tickers:
            symbol = t['symbol'].lower()
            volume_24h = float(t['quoteVolume'])
            if volume_24h >= VOLUME_THRESHOLD_USDT:
                active.add(symbol)
        logger.info(f"Active symbols: {len(active)} (volume > {VOLUME_THRESHOLD_USDT:,} USDT)")
        return active

    async def get_active_symbols(self) -> Set[str]:
        tickers = await self.get_24h_ticker()
        return self.filter_active_symbols(tickers)

# ==================== 基于时间窗口的异动检测器 ====================
class TradeRecord:
    __slots__ = ('timestamp', 'price', 'volume', 'is_buyer_maker')
    def __init__(self, timestamp: float, price: float, volume: float, is_buyer_maker: bool):
        self.timestamp = timestamp
        self.price = price
        self.volume = volume
        self.is_buyer_maker = is_buyer_maker

class SymbolDetector:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.recent_trades = deque()          # 存储最近几秒的成交
        self.historical_volume = deque()       # 元素为 (timestamp, volume)
        self.historical_total_volume = 0.0
        self.last_signal_time = 0.0

    def add_trade(self, timestamp: float, price: float, volume: float, is_buyer_maker: bool):
        # 加入最近成交队列
        self.recent_trades.append(TradeRecord(timestamp, price, volume, is_buyer_maker))
        # 清理超过聚合窗口 + 1秒的旧记录
        cutoff = timestamp - (AGGREGATION_WINDOW_SECONDS + 1)
        while self.recent_trades and self.recent_trades[0].timestamp < cutoff:
            self.recent_trades.popleft()

        # 加入历史成交量队列
        self.historical_volume.append((timestamp, volume))
        self.historical_total_volume += volume
        # 清理超过历史窗口的旧数据
        hist_cutoff = timestamp - HISTORICAL_WINDOW_SECONDS
        while self.historical_volume and self.historical_volume[0][0] < hist_cutoff:
            old_ts, old_vol = self.historical_volume.popleft()
            self.historical_total_volume -= old_vol

    def get_window_aggregate(self) -> Optional[Dict]:
        """获取当前时间窗口内的聚合数据（累计成交量、最高价、最低价、开盘价、收盘价）"""
        if len(self.recent_trades) == 0:
            return None
        now = self.recent_trades[-1].timestamp
        window_start = now - AGGREGATION_WINDOW_SECONDS
        window_trades = [t for t in self.recent_trades if t.timestamp >= window_start]
        if not window_trades:
            return None
        total_vol = sum(t.volume for t in window_trades)
        max_price = max(t.price for t in window_trades)
        min_price = min(t.price for t in window_trades)
        open_price = window_trades[0].price
        close_price = window_trades[-1].price
        return {
            'total_volume': total_vol,
            'max_price': max_price,
            'min_price': min_price,
            'open_price': open_price,
            'close_price': close_price,
            'window_start': window_start,
            'window_end': now
        }

    def get_historical_avg_volume_per_second(self) -> float:
        """返回过去 HISTORICAL_WINDOW_SECONDS 秒内的平均每秒成交量"""
        if self.historical_total_volume == 0 or len(self.historical_volume) == 0:
            return 0.0
        # 实际时间跨度（秒）
        if len(self.historical_volume) >= 2:
            time_span = self.historical_volume[-1][0] - self.historical_volume[0][0]
            if time_span > 0:
                return self.historical_total_volume / time_span
        return self.historical_total_volume / HISTORICAL_WINDOW_SECONDS

    def check_signal(self, current_time: float) -> Optional[Tuple[str, Dict]]:
        """检查当前窗口是否满足信号条件，返回 (signal_type, details) 或 None"""
        if current_time - self.last_signal_time < SIGNAL_COOLDOWN_SECONDS:
            return None

        window = self.get_window_aggregate()
        if not window:
            return None

        # 计算历史平均每秒成交量
        avg_vol_per_sec = self.get_historical_avg_volume_per_second()
        if avg_vol_per_sec == 0:
            return None

        # 计算窗口期望成交量（历史平均 * 窗口时长）
        expected_vol = avg_vol_per_sec * AGGREGATION_WINDOW_SECONDS
        if expected_vol == 0:
            return None

        # 实际成交量与期望成交量的比值
        volume_ratio = window['total_volume'] / expected_vol
        if volume_ratio < VOLUME_RATIO_THRESHOLD:
            return None

        # 价格变化判断
        open_price = window['open_price']
        close_price = window['close_price']
        price_change_pct = (close_price - open_price) / open_price * 100

        if abs(price_change_pct) < MIN_PRICE_CHANGE_PERCENT:
            return None

        if price_change_pct > 0:
            signal_type = "BULLISH_SPIKE"
        else:
            signal_type = "BEARISH_SPIKE"

        details = {
            "price": close_price,
            "window_volume": window['total_volume'],
            "price_change": price_change_pct / 100,
            "volume_ratio": volume_ratio,
            "expected_vol": expected_vol
        }
        return (signal_type, details)

# ==================== 检测器管理器 ====================
class DetectorManager:
    def __init__(self):
        self.detectors: Dict[str, SymbolDetector] = {}
        self.detectors_lock = asyncio.Lock()
        self.cooldown: Dict[Tuple[str, str], float] = {}
        self.cooldown_lock = asyncio.Lock()
        self.csv_lock = asyncio.Lock()
        self._init_csv_file()

    def _init_csv_file(self):
        if not os.path.isfile(CSV_FILE_PATH):
            try:
                with open(CSV_FILE_PATH, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        "北京时间", "币种", "信号类型", "价格", "窗口累计成交量",
                        "价格变化百分比", "成交量倍数", "Unix时间戳"
                    ])
                logger.info(f"CSV file created: {CSV_FILE_PATH}")
            except Exception as e:
                logger.error(f"Failed to create CSV file: {e}")

    async def get_or_create(self, symbol: str) -> SymbolDetector:
        async with self.detectors_lock:
            if symbol not in self.detectors:
                self.detectors[symbol] = SymbolDetector(symbol)
            return self.detectors[symbol]

    async def process_trade(self, symbol: str, price: float, quantity: float, is_buyer_maker: bool):
        detector = await self.get_or_create(symbol)
        now = time.time()
        detector.add_trade(now, price, quantity, is_buyer_maker)
        signal = detector.check_signal(now)
        if signal:
            signal_type, details = signal
            async with self.cooldown_lock:
                self.cooldown[(symbol, signal_type)] = now
            await self._notify(symbol, signal_type, details)

    async def _notify(self, symbol: str, signal_type: str, details: dict):
        beijing_time = get_beijing_time()
        price = details["price"]
        window_volume = details["window_volume"]
        price_change_pct = details["price_change"] * 100
        volume_ratio = details["volume_ratio"]

        signal_map = {
            "BULLISH_SPIKE": "🔥 极速追涨",
            "BEARISH_SPIKE": "💀 极速杀跌"
        }
        signal_cn = signal_map.get(signal_type, signal_type)

        title = f"[{signal_cn}] {symbol.upper()}"
        desp = (
            f"### {signal_cn}\n"
            f"- **币种**：{symbol.upper()}\n"
            f"- **时间**：{beijing_time}\n"
            f"- **价格**：{price:.8f}\n"
            f"- **窗口累计成交量**：{window_volume:.2f}\n"
            f"- **价格变化**：{price_change_pct:+.2f}%\n"
            f"- **成交量倍数**：{volume_ratio:.1f}x"
        )

        asyncio.create_task(self._send_serverchan(title, desp))
        asyncio.create_task(self._write_csv(beijing_time, symbol, signal_type, price, window_volume, price_change_pct, volume_ratio))

    async def _send_serverchan(self, title: str, desp: str):
        if not SERVER_CHAN_KEY:
            return
        url = f"https://sctapi.ftqq.com/{SERVER_CHAN_KEY}.send"
        data = {"title": title, "desp": desp}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=data) as resp:
                    if resp.status == 200:
                        logger.info(f"Server酱推送成功: {title}")
                    else:
                        logger.warning(f"Server酱推送失败: {resp.status}")
        except Exception as e:
            logger.error(f"Server酱推送异常: {e}")

    async def _write_csv(self, beijing_time: str, symbol: str, signal_type: str,
                         price: float, volume: float, price_change_pct: float, volume_ratio: float):
        async with self.csv_lock:
            try:
                with open(CSV_FILE_PATH, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        beijing_time,
                        symbol.upper(),
                        signal_type,
                        f"{price:.8f}",
                        f"{volume:.2f}",
                        f"{price_change_pct:+.2f}",
                        f"{volume_ratio:.1f}",
                        int(time.time())
                    ])
            except Exception as e:
                logger.error(f"Failed to write CSV: {e}")

# ==================== WebSocket 连接管理 ====================
class ConnectionHandler:
    def __init__(self, streams: List[str], detector_manager: DetectorManager):
        self.streams = streams
        self.url = f"{WEBSOCKET_BASE_URL}/stream?streams={'/'.join(streams)}"
        self.websocket = None
        self.queue = Queue(maxsize=MESSAGE_QUEUE_SIZE)
        self.running = False
        self.detector_manager = detector_manager

    async def _producer(self):
        try:
            async for message in self.websocket:
                try:
                    self.queue.put_nowait(message)
                except QueueFull:
                    logger.warning(f"Queue full for {self.url[:50]}")
        except websockets.ConnectionClosed:
            logger.error("Connection closed")
        except Exception as e:
            logger.exception(f"Producer error: {e}")
        finally:
            self.running = False
            asyncio.create_task(self._reconnect())

    async def _consumer(self):
        while self.running:
            try:
                message = await self.queue.get()
                await self._handle_message(message)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Consumer error: {e}")

    async def _handle_message(self, raw_message: str):
        try:
            data = json.loads(raw_message)
            stream = data.get('stream')
            if not stream:
                return
            symbol = stream.split('@')[0]
            trade = data.get('data', {})
            if 'p' in trade:
                price = float(trade['p'])
                quantity = float(trade['q'])
                is_buyer_maker = trade.get('m', True)
                await self.detector_manager.process_trade(symbol, price, quantity, is_buyer_maker)
        except Exception as e:
            logger.exception(f"Error handling message: {e}")

    async def _keepalive(self):
        while self.running and self.websocket:
            await asyncio.sleep(KEEPALIVE_INTERVAL)
            try:
                if self.websocket.open:
                    await self.websocket.ping()
            except:
                pass

    async def connect(self):
        self.running = True
        try:
            logger.info(f"Connecting to {self.url[:80]}...")
            self.websocket = await websockets.connect(
                self.url,
                ping_interval=KEEPALIVE_INTERVAL,
                ping_timeout=10,
                close_timeout=10
            )
            producer = asyncio.create_task(self._producer())
            consumer = asyncio.create_task(self._consumer())
            keepalive = asyncio.create_task(self._keepalive())
            await asyncio.gather(producer, consumer, keepalive)
        except Exception as e:
            logger.error(f"Connection error: {e}")
            self.running = False
            await self._reconnect()

    async def _reconnect(self):
        logger.info(f"Reconnecting in {RECONNECT_DELAY}s...")
        await asyncio.sleep(RECONNECT_DELAY)
        if not self.running:
            return
        asyncio.create_task(self.connect())

    async def close(self):
        self.running = False
        if self.websocket:
            await self.websocket.close()

class WebSocketManager:
    def __init__(self, detector_manager: DetectorManager):
        self.detector_manager = detector_manager
        self.connections: List[ConnectionHandler] = []
        self.active_symbols: Set[str] = set()

    async def update_symbols(self, symbols: Set[str]):
        for conn in self.connections:
            await conn.close()
        self.connections.clear()
        self.active_symbols = symbols
        if not symbols:
            return
        symbol_list = list(symbols)
        groups = [symbol_list[i:i+MAX_STREAMS_PER_CONNECTION] for i in range(0, len(symbol_list), MAX_STREAMS_PER_CONNECTION)]
        for group in groups:
            streams = [f"{sym}@trade" for sym in group]
            handler = ConnectionHandler(streams, self.detector_manager)
            self.connections.append(handler)
            asyncio.create_task(handler.connect())
        logger.info(f"Created {len(self.connections)} connections for {len(symbols)} symbols")

    async def close_all(self):
        for conn in self.connections:
            await conn.close()

# ==================== 主程序 ====================
async def refresh_symbols_task(rest_client: BinanceRestClient, ws_manager: WebSocketManager):
    while True:
        try:
            logger.info("Refreshing active symbols...")
            active = await rest_client.get_active_symbols()
            if active != ws_manager.active_symbols:
                logger.info(f"Symbols changed. New count: {len(active)}")
                await ws_manager.update_symbols(active)
            else:
                logger.debug(f"Symbols unchanged: {len(active)} active")
        except Exception as e:
            logger.exception(f"Error refreshing symbols: {e}")
        await asyncio.sleep(SYMBOL_REFRESH_INTERVAL)

async def main():
    logger.info("Starting Binance Futures Scraper Monitor (optimized)...")
    detector_manager = DetectorManager()
    ws_manager = WebSocketManager(detector_manager)

    async with BinanceRestClient() as rest_client:
        initial_symbols = await rest_client.get_active_symbols()
        await ws_manager.update_symbols(initial_symbols)
        refresh_task = asyncio.create_task(refresh_symbols_task(rest_client, ws_manager))
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            refresh_task.cancel()
            await ws_manager.close_all()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
