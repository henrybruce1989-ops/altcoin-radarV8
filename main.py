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
# 币安 API 端点
REST_BASE_URL = "https://fapi.binance.com"
WEBSOCKET_BASE_URL = "wss://fstream.binance.com"

# 筛选条件：24h 交易额阈值 (USDT)
VOLUME_THRESHOLD_USDT = 8_000_000  # 800万

# 监控参数
VOLUME_SPIKE_MULTIPLIER = 5        # 成交量暴增倍数
CONSECUTIVE_SPIKES = 3             # 连续暴增笔数阈值
PRICE_CHANGE_THRESHOLD = 0.001     # 价格变化阈值 (0.1%)

# WebSocket 连接参数
MAX_STREAMS_PER_CONNECTION = 50    # 每个连接最多订阅的币种数
MESSAGE_QUEUE_SIZE = 200           # 每个连接的队列大小
RECONNECT_DELAY = 5                # 重连延迟 (秒)
KEEPALIVE_INTERVAL = 30            # 心跳间隔 (秒)

# 全局控制
SYMBOL_REFRESH_INTERVAL = 600      # 活跃币种列表刷新间隔 (秒) 10分钟
LOG_LEVEL = "INFO"

# Server酱推送
SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY", "sctp14659thuntd89pzhhlsmbwynooxu")  # 从环境变量获取
SIGNAL_COOLDOWN_SECONDS = 30       # 同一币种同类型信号冷却时间 (秒)

# CSV 记录文件路径 (Railway 部署时请确保目录可写)
CSV_FILE_PATH = "signals.csv"      # 默认在当前目录

# ==================== 日志配置 ====================
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# 北京时间时区
BEIJING_TZ = timezone(timedelta(hours=8))

def get_beijing_time() -> str:
    """返回格式化的北京时间字符串"""
    return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")

# ==================== 币种筛选（REST） ====================
class BinanceRestClient:
    def __init__(self):
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    async def get_24h_ticker(self) -> List[Dict]:
        """获取所有永续合约的24h行情"""
        url = f"{REST_BASE_URL}/fapi/v1/ticker/24hr"
        try:
            async with self.session.get(url) as resp:
                data = await resp.json()
                return data
        except Exception as e:
            logger.error(f"Failed to fetch 24h ticker: {e}")
            return []

    def filter_active_symbols(self, tickers: List[Dict]) -> Set[str]:
        """筛选交易额 > 阈值的币种，返回币种符号集合（如 'btcusdt'）"""
        active = set()
        for t in tickers:
            symbol = t['symbol'].lower()
            volume_24h = float(t['quoteVolume'])  # 已是以 USDT 计价的交易额
            if volume_24h >= VOLUME_THRESHOLD_USDT:
                active.add(symbol)
        logger.info(f"Active symbols: {len(active)} (volume > {VOLUME_THRESHOLD_USDT:,} USDT)")
        return active

    async def get_active_symbols(self) -> Set[str]:
        tickers = await self.get_24h_ticker()
        return self.filter_active_symbols(tickers)

# ==================== 异动检测器 ====================
class SymbolDetector:
    """单个币种的异动检测器"""
    def __init__(self, symbol: str, window_size: int = 60):
        self.symbol = symbol
        self.recent_volumes = deque(maxlen=window_size)
        self.consecutive_spike_count = 0
        self.last_price = None

    def process_trade(self, price: float, quantity: float, is_buyer_maker: bool):
        """
        处理一笔成交数据
        返回: (signal_type, details) 或 None
        signal_type: "BULLISH_SPIKE", "BEARISH_SPIKE", "FALSE_BREAKOUT"
        """
        # 更新成交量窗口
        self.recent_volumes.append(quantity)
        avg_volume = sum(self.recent_volumes) / len(self.recent_volumes) if self.recent_volumes else quantity
        is_spike = quantity > avg_volume * VOLUME_SPIKE_MULTIPLIER

        # 价格变化
        price_change = 0
        if self.last_price is not None:
            price_change = (price - self.last_price) / self.last_price
        self.last_price = price

        # 连续暴增计数
        if is_spike:
            self.consecutive_spike_count += 1
        else:
            self.consecutive_spike_count = 0

        # 判断信号
        signal = None
        if self.consecutive_spike_count >= CONSECUTIVE_SPIKES:
            if price_change > PRICE_CHANGE_THRESHOLD:
                signal = ("BULLISH_SPIKE", {
                    "price": price,
                    "volume": quantity,
                    "price_change": price_change,
                    "consecutive": self.consecutive_spike_count
                })
            elif price_change < -PRICE_CHANGE_THRESHOLD:
                signal = ("BEARISH_SPIKE", {
                    "price": price,
                    "volume": quantity,
                    "price_change": price_change,
                    "consecutive": self.consecutive_spike_count
                })
        elif is_spike and abs(price_change) < PRICE_CHANGE_THRESHOLD / 2:
            # 放量但价格几乎没动 -> 可能出货/吸筹
            signal = ("FALSE_BREAKOUT", {
                "price": price,
                "volume": quantity,
                "price_change": price_change,
                "consecutive": 0
            })

        return signal


class DetectorManager:
    """管理所有币种的检测器实例，处理推送与CSV记录"""
    def __init__(self):
        self.detectors: Dict[str, SymbolDetector] = {}
        self.detectors_lock = asyncio.Lock()
        # 冷却字典: (symbol, signal_type) -> last_push_timestamp
        self.cooldown: Dict[Tuple[str, str], float] = {}
        self.cooldown_lock = asyncio.Lock()
        # CSV 写入锁
        self.csv_lock = asyncio.Lock()
        self._init_csv_file()

    def _init_csv_file(self):
        """确保CSV文件存在并写入表头"""
        if not os.path.isfile(CSV_FILE_PATH):
            try:
                with open(CSV_FILE_PATH, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        "北京时间", "币种", "信号类型", "价格", "成交量",
                        "价格变化百分比", "连续暴增笔数", "Unix时间戳"
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
        signal = detector.process_trade(price, quantity, is_buyer_maker)
        if signal:
            signal_type, details = signal
            await self._notify(symbol, signal_type, details)

    async def _notify(self, symbol: str, signal_type: str, details: dict):
        """推送 Server酱 并记录 CSV（带冷却）"""
        # 冷却检查
        key = (symbol, signal_type)
        async with self.cooldown_lock:
            last_push = self.cooldown.get(key, 0)
            now = time.time()
            if now - last_push < SIGNAL_COOLDOWN_SECONDS:
                logger.debug(f"Cooldown active for {symbol} {signal_type}, skip push")
                return
            self.cooldown[key] = now

        # 构造消息内容
        beijing_time = get_beijing_time()
        price = details["price"]
        volume = details["volume"]
        price_change_pct = details["price_change"] * 100
        consecutive = details.get("consecutive", 0)

        # 信号中文映射
        signal_map = {
            "BULLISH_SPIKE": "🔥 极速追涨",
            "BEARISH_SPIKE": "💀 极速杀跌",
            "FALSE_BREAKOUT": "⚠️ 放量滞涨"
        }
        signal_cn = signal_map.get(signal_type, signal_type)

        # 推送内容（Server酱）
        title = f"[{signal_cn}] {symbol.upper()}"
        desp = (
            f"### {signal_cn}\n"
            f"- **币种**：{symbol.upper()}\n"
            f"- **时间**：{beijing_time}\n"
            f"- **价格**：{price:.8f}\n"
            f"- **成交量**：{volume:.4f}\n"
            f"- **价格变化**：{price_change_pct:+.2f}%\n"
            f"- **连续暴增**：{consecutive} 笔"
        )

        # 异步推送 Server酱
        asyncio.create_task(self._send_serverchan(title, desp))

        # 异步记录 CSV
        asyncio.create_task(self._write_csv(beijing_time, symbol, signal_type, price, volume, price_change_pct, consecutive))

    async def _send_serverchan(self, title: str, desp: str):
        """通过 Server酱 发送通知（异步）"""
        if not SERVER_CHAN_KEY:
            logger.debug("SERVER_CHAN_KEY not set, skip push")
            return
        url = f"https://sctapi.ftqq.com/{SERVER_CHAN_KEY}.send"
        data = {"title": title, "desp": desp}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=data) as resp:
                    if resp.status == 200:
                        logger.info(f"Server酱推送成功: {title}")
                    else:
                        logger.warning(f"Server酱推送失败: {resp.status} {await resp.text()}")
        except Exception as e:
            logger.error(f"Server酱推送异常: {e}")

    async def _write_csv(self, beijing_time: str, symbol: str, signal_type: str,
                         price: float, volume: float, price_change_pct: float, consecutive: int):
        """追加一条信号记录到 CSV 文件"""
        async with self.csv_lock:
            try:
                with open(CSV_FILE_PATH, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        beijing_time,
                        symbol.upper(),
                        signal_type,
                        f"{price:.8f}",
                        f"{volume:.4f}",
                        f"{price_change_pct:+.2f}",
                        consecutive,
                        int(time.time())
                    ])
                logger.debug(f"CSV record added: {symbol} {signal_type}")
            except Exception as e:
                logger.error(f"Failed to write CSV: {e}")

# ==================== WebSocket 连接管理 ====================
class ConnectionHandler:
    """单个WebSocket连接的管理"""
    def __init__(self, streams: List[str], detector_manager: DetectorManager):
        self.streams = streams          # 如 ["btcusdt@trade", "ethusdt@trade"]
        self.url = f"{WEBSOCKET_BASE_URL}/stream?streams={'/'.join(streams)}"
        self.websocket = None
        self.queue = Queue(maxsize=MESSAGE_QUEUE_SIZE)
        self.running = False
        self.detector_manager = detector_manager
        self.reconnect_task = None

    async def _producer(self):
        """接收消息并放入队列"""
        try:
            async for message in self.websocket:
                try:
                    self.queue.put_nowait(message)
                except QueueFull:
                    logger.warning(f"Queue full for connection {self.url[:50]}... dropping message")
        except websockets.ConnectionClosed as e:
            logger.error(f"Connection closed: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error in producer: {e}")
        finally:
            self.running = False
            # 触发重连
            asyncio.create_task(self._reconnect())

    async def _consumer(self):
        """从队列取消息并处理"""
        while self.running:
            try:
                message = await self.queue.get()
                await self._handle_message(message)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Error processing message: {e}")

    async def _handle_message(self, raw_message: str):
        """解析消息并分发到检测器"""
        try:
            data = json.loads(raw_message)
            stream_name = data.get('stream')
            if not stream_name:
                return
            # 提取币种符号，例如 "btcusdt@trade" -> "btcusdt"
            symbol = stream_name.split('@')[0]
            trade_data = data.get('data', {})
            if 'p' in trade_data:  # trade 流格式
                price = float(trade_data['p'])
                quantity = float(trade_data['q'])
                is_buyer_maker = trade_data.get('m', True)  # m=True 表示买方挂单（即卖方吃单）
                # 注意：币安 futures 中 m 表示是否为买方挂单（maker），所以 is_taker_buy = not m
                await self.detector_manager.process_trade(symbol, price, quantity, is_buyer_maker)
        except Exception as e:
            logger.exception(f"Error handling message: {e}")

    async def _keepalive(self):
        """定期发送ping保持连接"""
        while self.running and self.websocket:
            try:
                await asyncio.sleep(KEEPALIVE_INTERVAL)
                if self.websocket.open:
                    await self.websocket.ping()
            except Exception:
                pass

    async def connect(self):
        """建立连接并启动生产消费循环"""
        self.running = True
        try:
            logger.info(f"Connecting to {self.url[:80]}...")
            self.websocket = await websockets.connect(
                self.url,
                ping_interval=KEEPALIVE_INTERVAL,
                ping_timeout=10,
                close_timeout=10
            )
            # 启动生产者、消费者、心跳任务
            producer_task = asyncio.create_task(self._producer())
            consumer_task = asyncio.create_task(self._consumer())
            keepalive_task = asyncio.create_task(self._keepalive())
            await asyncio.gather(producer_task, consumer_task, keepalive_task)
        except Exception as e:
            logger.error(f"Connection error: {e}")
            self.running = False
            await self._reconnect()

    async def _reconnect(self):
        """重连逻辑"""
        logger.info(f"Will reconnect in {RECONNECT_DELAY}s...")
        await asyncio.sleep(RECONNECT_DELAY)
        if not self.running:
            return
        asyncio.create_task(self.connect())

    async def close(self):
        """关闭连接"""
        self.running = False
        if self.websocket:
            await self.websocket.close()


class WebSocketManager:
    """管理所有连接"""
    def __init__(self, detector_manager: DetectorManager):
        self.detector_manager = detector_manager
        self.connections: List[ConnectionHandler] = []
        self.active_symbols: Set[str] = set()

    async def update_symbols(self, symbols: Set[str]):
        """根据活跃币种列表重新建立连接"""
        # 关闭旧连接
        for conn in self.connections:
            await conn.close()
        self.connections.clear()
        self.active_symbols = symbols

        if not symbols:
            logger.warning("No active symbols to monitor")
            return

        # 将币种分组，每个连接最多 MAX_STREAMS_PER_CONNECTION 个stream
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
    """定期更新活跃币种列表"""
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
    logger.info("Starting Binance Futures Scraper Monitor...")

    detector_manager = DetectorManager()
    ws_manager = WebSocketManager(detector_manager)

    async with BinanceRestClient() as rest_client:
        # 初始获取活跃币种
        initial_symbols = await rest_client.get_active_symbols()
        await ws_manager.update_symbols(initial_symbols)

        # 启动定期刷新任务
        refresh_task = asyncio.create_task(refresh_symbols_task(rest_client, ws_manager))

        # 保持主程序运行
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
