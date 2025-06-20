# future_trade/backtest/engine.py

import pandas as pd
import numpy as np
import asyncio
import time
import datetime
from typing import Dict, List, Optional, Tuple, Any
from decimal import (
    Decimal,
    ROUND_DOWN,
    ROUND_HALF_UP,
)  # 添加 ROUND_HALF_UP 用于价格精度
import traceback  # 导入 traceback

# 确保能正确导入项目模块
try:
    from database.sqlite import SQLiteDB
    from trading.api import ExchangeInterface  # 用于获取数据
    from strategies.mystrategy import MyStrategy  # 或者其他策略基类/接口
    from utils.logger import logger
    from backtest.results import calculate_metrics  # 只导入需要的函数
except ImportError as imp_err:
    logger.error(
        f"导入模块失败: {imp_err}。请确保在项目根目录下运行，或检查 PYTHONPATH。"
    )
    import sys

    sys.exit(1)


# 定义精度常量 (可以根据需要调整)
# 使用 Decimal 更精确，但 float 在简单回测中也可接受
# PRICE_PRECISION = Decimal('0.01') # 价格精度，例如 USDT 对
# AMOUNT_PRECISION = Decimal('0.001') # 数量精度，例如 BTC
# 这里暂时用 float，如果需要高精度再切换
DEFAULT_PRICE_PRECISION = 4  # 小数位数
DEFAULT_AMOUNT_PRECISION = 6  # 小数位数


class BacktestEngine:
    """
    事件驱动的回测引擎 (异步数据准备 + 同步事件循环)。
    模拟期货交易。
    """

    def __init__(
        self,
        strategy_instance: MyStrategy,
        symbols: List[str],
        start_date: str,
        end_date: str,
        timeframe: str,
        initial_capital: float,
        leverage: int,
        commission_rate: float,  # 手续费率 (例如 0.0004 for 0.04%)
        slippage_pct: float,  # 滑点百分比 (例如 0.0005 for 0.05%)
        db_path: str,
        exchange_name: Optional[str] = None,  # 仅在需要获取数据时提供
        api_key: Optional[str] = None,  # 仅在需要获取数据时提供
        secret_key: Optional[str] = None,  # 仅在需要获取数据时提供
        testnet: bool = False,  # 仅在需要获取数据时提供
        fetch_missing_data: bool = True,  # 是否尝试获取缺失数据
        position_sizing_pct: float = 0.1,
        proxy_url: Optional[str] = None,  # <--- 新增 proxy_url 参数
    ):  # 新增：默认开仓比例
        """
        初始化回测引擎。
        ... (参数说明不变) ...
        :param position_sizing_pct: 开仓时使用的权益百分比 (乘以杠杆后计算名义价值)。
        """
        self.strategy = strategy_instance
        self.symbols = symbols
        try:
            self.start_date = pd.to_datetime(start_date, utc=True)
            self.end_date = pd.to_datetime(end_date, utc=True)
            if self.end_date <= self.start_date:
                raise ValueError("结束日期必须晚于开始日期")
        except Exception as date_e:
            logger.error(f"日期格式错误或无效: {date_e}")
            raise ValueError(f"无效的日期: {date_e}")

        self.timeframe = timeframe
        self.initial_capital = float(initial_capital)
        self.leverage = int(leverage)
        self.commission_rate = float(commission_rate)
        self.slippage_pct = float(slippage_pct)
        self.db_path = db_path
        self.exchange_name = exchange_name
        self.api_key = api_key
        self.secret_key = secret_key
        self.testnet = testnet
        self.fetch_missing_data = fetch_missing_data
        self.position_sizing_pct = float(position_sizing_pct)
        self.proxy_url = proxy_url
        if not (0 < self.position_sizing_pct <= 1):
            logger.warning(
                f"无效的 position_sizing_pct ({self.position_sizing_pct})，将使用默认值 0.1"
            )
            self.position_sizing_pct = 0.1

        # --- 内部状态 ---
        self.db: Optional[SQLiteDB] = None
        self.exchange_interface: Optional[ExchangeInterface] = None
        self.all_market_data: Optional[pd.DataFrame] = None  # 合并后的多交易对数据
        self.event_timestamps: Optional[pd.Index] = None  # 驱动事件循环的时间戳

        self.cash: float = self.initial_capital
        self.equity: float = self.initial_capital
        self.positions: Dict[str, Dict[str, Any]] = (
            {}
        )  # key=symbol, value={'side': 'long'/'short', 'amount': float, 'entry_price': float, 'value': float, 'entry_ts': Timestamp}
        self.equity_curve: Dict[pd.Timestamp, float] = {}  # 记录每个时间点的权益
        self.trades: List[Dict[str, Any]] = []  # 记录模拟交易

        # --- 辅助 ---
        try:
            self.timeframe_delta = pd.Timedelta(self.timeframe)  # K线周期的时间增量
        except ValueError as td_e:
            logger.error(f"无法解析时间周期 '{self.timeframe}': {td_e}")
            raise ValueError(f"无效的时间周期: {self.timeframe}")

        # 假设的价格和数量精度 (可以从交易所获取，但回测中简化)
        self.price_precision = DEFAULT_PRICE_PRECISION
        self.amount_precision = DEFAULT_AMOUNT_PRECISION

    async def _initialize_connections(self):
        """初始化数据库连接和交易所接口 (如果需要)"""
        logger.info("初始化数据库连接...")
        try:
            # check_same_thread=False 通常用于多线程访问，回测是单事件循环，但SQLiteDB内部实现可能需要
            self.db = SQLiteDB(db_path=self.db_path, check_same_thread=True)
        except Exception as db_e:
            logger.error(f"初始化数据库连接失败: {db_e}", exc_info=True)
            raise ConnectionError("数据库未能成功初始化")  # 抛出异常终止

        if self.fetch_missing_data:
            if not all([self.exchange_name, self.api_key, self.secret_key]):
                logger.warning(
                    "需要获取数据，但未提供完整的交易所名称、API Key 和 Secret Key。将仅使用数据库数据。"
                )
                self.fetch_missing_data = False  # 无法获取，禁用该功能
            else:
                logger.info(
                    f"初始化交易所接口 '{self.exchange_name}' 用于获取缺失数据..."
                )
                try:
                    self.exchange_interface = ExchangeInterface(
                        exchange_name=self.exchange_name,
                        api=self.api_key,
                        secret=self.secret_key,
                        market_type="future",  # 假设回测期货
                        testnet=self.testnet,
                        proxy_url=self.proxy_url,  # <--- 传递代理 URL
                    )
                    await self.exchange_interface.initialize()  # 异步初始化
                    logger.info("交易所接口初始化成功。")
                    # 可以尝试获取精度信息，但 fetch_ohlcv 不一定需要 markets 加载完成
                    # self.price_precision = self.exchange_interface._get_market_precision(self.symbols[0])['price'] or self.price_precision
                    # self.amount_precision = self.exchange_interface._get_market_precision(self.symbols[0])['amount'] or self.amount_precision
                except Exception as e:
                    logger.error(f"初始化交易所接口失败: {e}。将仅使用数据库数据。")
                    self.fetch_missing_data = False
                    self.exchange_interface = None  # 确保接口不可用

    async def _fetch_and_store_data(
        self, symbol: str, start_ts_ms: int, end_ts_ms: int
    ) -> Optional[pd.DataFrame]:
        """尝试从交易所获取指定时间范围的K线数据并存入数据库"""
        if (
            not self.exchange_interface
            or not self.db
            or not self.exchange_interface.exchange
        ):
            logger.warning(
                f"[{symbol}] 无法获取数据：交易所接口或数据库未初始化或接口内部实例丢失。"
            )
            return None

        logger.info(
            f"[{symbol}] 尝试从交易所获取 {self.timeframe} K线数据 ({pd.to_datetime(start_ts_ms, unit='ms', utc=True)} to {pd.to_datetime(end_ts_ms, unit='ms', utc=True)})"
        )

        all_klines = []
        current_start_ts = start_ts_ms
        fetch_limit = 1000  # 每次获取的最大数量 (交易所限制)
        max_retries = 3
        retry_delay = 5  # 秒

        while current_start_ts <= end_ts_ms:
            retries = 0
            fetched_successfully = False
            while retries < max_retries and not fetched_successfully:
                try:
                    # 确保使用 await 调用异步方法
                    logger.debug(
                        f"[{symbol}] Fetching from {pd.to_datetime(current_start_ts, unit='ms', utc=True)} limit {fetch_limit}"
                    )
                    klines_chunk_raw = (
                        await self.exchange_interface.exchange.fetch_ohlcv(
                            symbol=self.exchange_interface._format_symbol(symbol),
                            timeframe=self.timeframe,
                            since=current_start_ts,
                            limit=fetch_limit,
                        )
                    )

                    # fetch_ohlcv 返回的是 list of lists: [[timestamp, open, high, low, close, volume], ...]
                    if isinstance(klines_chunk_raw, list) and klines_chunk_raw:
                        logger.debug(
                            f"[{symbol}] 获取到 {len(klines_chunk_raw)} 条原始 K 线数据"
                        )
                        # 过滤掉结束时间戳之后的数据 (如果交易所多返回了)
                        klines_chunk = [
                            k for k in klines_chunk_raw if k[0] <= end_ts_ms
                        ]

                        if not klines_chunk:  # 如果过滤后为空
                            logger.debug(
                                f"[{symbol}] 获取到的数据均晚于结束时间戳 {end_ts_ms}，停止获取。"
                            )
                            current_start_ts = end_ts_ms + 1  # 结束外层循环
                            fetched_successfully = True
                            break  # 结束重试循环

                        all_klines.extend(klines_chunk)
                        last_timestamp = klines_chunk[-1][0]

                        # 更新下一次请求的起始时间戳
                        # 避免重复获取最后一条，将 since 设为最后一条时间戳 + 1ms
                        current_start_ts = last_timestamp + 1

                        # 检查是否已经获取到或超过了结束时间
                        if last_timestamp >= end_ts_ms:
                            logger.debug(
                                f"[{symbol}] 已获取到或超过结束时间戳 {end_ts_ms}"
                            )
                            current_start_ts = end_ts_ms + 1  # 强制结束外层循环

                        fetched_successfully = True
                        await asyncio.sleep(0.5)  # 尊重交易所速率限制
                    else:
                        # 没有获取到数据，可能这个时间段就没有，前进一个时间周期
                        logger.debug(
                            f"[{symbol}] 在 {pd.to_datetime(current_start_ts, unit='ms', utc=True)} 未获取到数据，尝试前进一个时间周期"
                        )
                        timeframe_ms = self.timeframe_delta.total_seconds() * 1000
                        # 确保前进至少 1 毫秒，避免死循环
                        current_start_ts += max(timeframe_ms, 1)

                        # 如果前进后仍未超过结束时间，认为此次尝试成功（处理了空档期）
                        if current_start_ts <= end_ts_ms:
                            fetched_successfully = True
                        else:  # 前进后超过了结束时间，结束循环
                            logger.debug(f"[{symbol}] 前进后超过结束时间，停止获取。")
                            fetched_successfully = True  # 也算成功退出
                            current_start_ts = end_ts_ms + 1  # 结束外层循环

                except Exception as e:
                    retries += 1
                    logger.warning(
                        f"[{symbol}] 获取 K 线数据时出错 (第 {retries}/{max_retries} 次): {e}",
                        exc_info=True,
                    )
                    if retries >= max_retries:
                        logger.error(f"[{symbol}] 达到最大重试次数，放弃获取此分段。")
                        # 即使部分失败，也尝试返回已获取的数据
                        break  # 结束重试，继续外层 while 循环（如果时间没到）或退出
                    await asyncio.sleep(retry_delay * retries)  # 指数退避

            # 如果重试失败，也跳出外层循环
            if not fetched_successfully:
                break

        if not all_klines:
            logger.warning(f"[{symbol}] 未能从交易所获取到任何 K 线数据。")
            return pd.DataFrame()  # 返回空 DataFrame

        # --- 数据处理与存储 ---
        try:
            df = pd.DataFrame(
                all_klines,
                columns=["timestamp", "open", "high", "low", "close", "volume"],
            )
            if df.empty:
                return df  # 如果处理后为空

            # 确保时间戳唯一，并设置为索引
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            df = df.drop_duplicates(subset=["timestamp"], keep="last")  # 保留最新的记录
            df = df.set_index("timestamp").sort_index()  # 排序很重要

            # 确保数值类型
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df.dropna(
                subset=["open", "high", "low", "close"], inplace=True
            )  # OHLC 必须有值

            # 存储到数据库
            df_reset = df.reset_index()
            df_reset["timestamp"] = (
                df_reset["timestamp"].astype(np.int64) // 10**6
            )  # 转毫秒
            # 确保列存在
            cols_to_save = ["timestamp", "open", "high", "low", "close", "volume"]
            klines_list_for_db = df_reset[cols_to_save].values.tolist()

            if self.db.insert_klines(symbol, self.timeframe, klines_list_for_db):
                logger.info(
                    f"[{symbol}] 成功将 {len(klines_list_for_db)} 条获取到的 K 线存入数据库。"
                )
                # 可选：维护数据库大小
                # self.db.maintain_klines_limit(symbol, self.timeframe, limit=5000)
            else:
                logger.warning(f"[{symbol}] 存入数据库失败。")

            return df

        except Exception as process_e:
            logger.error(
                f"[{symbol}] 处理或存储获取到的 K 线数据时出错: {process_e}",
                exc_info=True,
            )
            return None

    async def _prepare_data(self):
        """
        (异步) 准备回测所需的所有市场数据。
        """
        await self._initialize_connections()  # 初始化连接放在这里

        all_data_dict = {}
        start_ts_ms = int(self.start_date.timestamp() * 1000)
        end_ts_ms = int(self.end_date.timestamp() * 1000)

        # 1. & 2. 加载和获取数据
        fetch_tasks = []
        for symbol in self.symbols:
            # 准备数据获取的任务
            fetch_tasks.append(
                self._load_and_fetch_symbol_data(symbol, start_ts_ms, end_ts_ms)
            )

        # 并发执行所有 symbol 的数据加载/获取任务
        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        # 处理结果
        for symbol, result in zip(self.symbols, results):
            if isinstance(result, Exception):
                logger.error(
                    f"[{symbol}] 准备数据时发生错误: {result}", exc_info=result
                )
            elif isinstance(result, pd.DataFrame) and not result.empty:
                all_data_dict[symbol] = result
            else:
                logger.warning(f"[{symbol}] 未能准备有效数据，将跳过该交易对。")

        if not all_data_dict:
            raise ValueError("未能为任何指定的交易对准备数据。回测无法进行。")

        # 3. 合并与对齐数据
        try:
            # 使用 pd.concat 创建多级索引 DataFrame (timestamp, symbol)
            self.all_market_data = (
                pd.concat(all_data_dict, names=["symbol", "timestamp"])
                .swaplevel()
                .sort_index()
            )

            # 检查合并后的索引
            if not isinstance(self.all_market_data.index, pd.MultiIndex):
                raise TypeError("合并后的数据索引不是 MultiIndex")
            ts_level = self.all_market_data.index.get_level_values("timestamp")
            if not isinstance(ts_level, pd.DatetimeIndex):
                raise TypeError(
                    f"合并后的时间戳索引不是 DatetimeIndex，而是 {type(ts_level)}"
                )
            if not ts_level.is_monotonic_increasing:
                logger.warning("合并后的时间戳索引不是单调递增的，尝试重新排序...")
                self.all_market_data.sort_index(inplace=True)

            # 获取所有独特的、排序的时间戳作为事件驱动点
            self.event_timestamps = (
                self.all_market_data.index.get_level_values("timestamp")
                .unique()
                .sort_values()
            )

            if self.event_timestamps.empty:
                raise ValueError("未能生成有效的事件时间戳列表。")

            logger.info(
                f"数据准备完成。共 {len(self.all_market_data)} 行数据，覆盖 {len(self.event_timestamps)} 个时间点。"
            )
            logger.debug(
                f"数据时间范围: 从 {self.event_timestamps.min()} 到 {self.event_timestamps.max()}"
            )

        except Exception as merge_e:
            logger.error(f"合并或处理数据时出错: {merge_e}", exc_info=True)
            raise ValueError(f"数据合并失败: {merge_e}")

    async def _load_and_fetch_symbol_data(
        self, symbol: str, start_ts_ms: int, end_ts_ms: int
    ) -> Optional[pd.DataFrame]:
        """(内部异步方法) 为单个 symbol 加载和获取数据"""
        logger.info(f"[{symbol}] 准备数据...")
        df_symbol: Optional[pd.DataFrame] = None

        # 尝试从数据库加载
        if self.db:
            try:
                df_symbol = self.db.get_klines(
                    symbol, self.timeframe, limit=10**9
                )  # 获取所有以便筛选
                if not df_symbol.empty:
                    if not isinstance(df_symbol.index, pd.DatetimeIndex):
                        logger.warning(
                            f"[{symbol}] DB 索引非 DatetimeIndex，尝试转换..."
                        )
                        df_symbol.index = pd.to_datetime(
                            df_symbol.index, unit="ms", utc=True
                        )
                    # 筛选日期范围
                    df_symbol = df_symbol[
                        (df_symbol.index >= self.start_date)
                        & (df_symbol.index <= self.end_date)
                    ].copy()
                    logger.info(
                        f"[{symbol}] 从数据库加载了 {len(df_symbol)} 条 K 线数据。"
                    )
                else:
                    logger.warning(f"[{symbol}] 数据库中无数据。")
            except Exception as db_load_e:
                logger.error(f"[{symbol}] 从数据库加载数据时出错: {db_load_e}")
                df_symbol = pd.DataFrame()  # 出错视为空

        # 检查是否需要从交易所获取
        needs_fetching = False
        fetch_start_ts = start_ts_ms
        fetch_end_ts = end_ts_ms

        if self.fetch_missing_data:
            if df_symbol is None or df_symbol.empty:
                needs_fetching = True
                logger.info(f"[{symbol}] 数据库无数据，需要从交易所获取整个范围。")
            else:
                # 检查数据是否覆盖了开始时间
                db_start_time = df_symbol.index.min()
                if (
                    db_start_time > self.start_date + self.timeframe_delta
                ):  # 加一个 buffer
                    needs_fetching = True
                    fetch_end_ts = (
                        int(db_start_time.timestamp() * 1000) - 1
                    )  # 获取到数据库数据之前
                    logger.info(
                        f"[{symbol}] 数据库数据起始晚于请求，需要获取 {pd.to_datetime(start_ts_ms, unit='ms', utc=True)} 到 {pd.to_datetime(fetch_end_ts, unit='ms', utc=True)} 的数据。"
                    )
                # 可以选择性地检查结束时间，但通常历史数据是完整的
                # db_end_time = df_symbol.index.max()
                # if db_end_time < self.end_date - self.timeframe_delta:
                #     needs_fetching = True # 也需要获取
                #     fetch_start_ts = int(db_end_time.timestamp() * 1000) + 1 # 从数据库数据之后获取
                #     fetch_end_ts = end_ts_ms
                #     logger.info(f"[{symbol}] 数据库数据结束早于请求，需要获取 {pd.to_datetime(fetch_start_ts, unit='ms', utc=True)} 到 {pd.to_datetime(fetch_end_ts, unit='ms', utc=True)} 的数据。")

        # 执行获取
        df_fetched = None
        if needs_fetching:
            df_fetched = await self._fetch_and_store_data(
                symbol, fetch_start_ts, fetch_end_ts
            )

        # 合并数据
        if df_fetched is not None and not df_fetched.empty:
            if df_symbol is None or df_symbol.empty:
                df_symbol = df_fetched[
                    (df_fetched.index >= self.start_date)
                    & (df_fetched.index <= self.end_date)
                ].copy()
            else:
                # 合并时要小心索引类型和重复
                df_fetched_filtered = df_fetched[
                    (df_fetched.index >= self.start_date)
                    & (df_fetched.index <= self.end_date)
                ]
                df_symbol = pd.concat([df_symbol, df_fetched_filtered]).sort_index()
                df_symbol = df_symbol[
                    ~df_symbol.index.duplicated(keep="last")
                ]  # 去重，保留最新的
            logger.info(
                f"[{symbol}] 合并数据库与获取的数据后，共有 {len(df_symbol)} 条。"
            )

        if df_symbol is not None and not df_symbol.empty:
            # 最后再做一次检查和筛选
            df_symbol = df_symbol[
                (df_symbol.index >= self.start_date)
                & (df_symbol.index <= self.end_date)
            ]
            if not isinstance(df_symbol.index, pd.DatetimeIndex):
                logger.error(f"[{symbol}] 最终数据索引类型错误，放弃该交易对。")
                return None
            return df_symbol
        else:
            return None

    def _run_event_loop(self):
        """
        (同步) 运行事件驱动的回测循环。
        """
        if self.all_market_data is None or self.event_timestamps is None:
            logger.error("数据未准备好，无法运行回测循环。")
            raise RuntimeError("数据未准备好")

        logger.info("开始运行回测事件循环...")
        start_run_time = time.time()

        # 确保第一个权益点在第一个事件时间戳之前
        first_event_ts = self.event_timestamps[0]
        self.equity_curve[first_event_ts - pd.Timedelta(microseconds=1)] = (
            self.initial_capital
        )  # 微小偏移避免重合

        for current_ts in self.event_timestamps:
            # current_ts 是当前 K 线的结束时间 (或 Pandas 时间戳索引)
            # logger.debug(f"Processing timestamp: {current_ts}")

            # 1. 获取当前时间点的市场数据快照
            market_data_snapshot: Dict[str, pd.DataFrame] = {}
            try:
                # 获取所有 symbols 在 current_ts 这一点的数据行
                current_bars = self.all_market_data.loc[current_ts]
                if isinstance(current_bars, pd.Series):  # 只有一个 symbol
                    symbol = current_bars.name
                    if symbol in self.symbols:
                        # 获取该 symbol 到 current_ts 的历史数据
                        symbol_df_full = self.all_market_data.xs(symbol, level="symbol")
                        market_data_snapshot[symbol] = symbol_df_full[
                            symbol_df_full.index <= current_ts
                        ].copy()
                elif isinstance(current_bars, pd.DataFrame):  # 多个 symbols
                    for symbol in current_bars.index:
                        if symbol in self.symbols:
                            symbol_df_full = self.all_market_data.xs(
                                symbol, level="symbol"
                            )
                            market_data_snapshot[symbol] = symbol_df_full[
                                symbol_df_full.index <= current_ts
                            ].copy()
            except KeyError:
                logger.warning(
                    f"[{current_ts}] 无法在 all_market_data 中找到数据行，跳过此时间点处理。"
                )
                # 更新权益曲线（保持上一个值）
                if self.equity_curve:
                    self.equity = self.equity_curve[max(self.equity_curve.keys())]
                self.equity_curve[current_ts] = self.equity
                continue
            except Exception as snap_e:
                logger.error(
                    f"[{current_ts}] 准备数据快照时出错: {snap_e}", exc_info=True
                )
                continue  # 跳过这个时间点

            # 确保传递给策略的数据非空且索引正确
            valid_snapshot = {}
            for symbol, df in market_data_snapshot.items():
                if not df.empty and isinstance(df.index, pd.DatetimeIndex):
                    valid_snapshot[symbol] = df
                # else: logger.warning(f"[{current_ts}|{symbol}] Snapshot is empty or has wrong index type.")

            if not valid_snapshot:
                # logger.debug(f"[{current_ts}] No valid market data snapshot for strategy.")
                # 仍然需要更新权益曲线
                self._update_portfolio(current_ts)
                continue

            # 2. 调用策略生成信号
            signals_df = None
            try:
                signals_df = self.strategy.generate_signals(valid_snapshot)
            except Exception as strat_e:
                logger.error(
                    f"[{current_ts}] 调用策略 {self.strategy.name} 出错: {strat_e}",
                    exc_info=True,
                )

            # 3. 处理信号并模拟交易
            if signals_df is not None and not signals_df.empty:
                # logger.debug(f"[{current_ts}] 收到信号:\n{signals_df}")
                for symbol, signal in signals_df.iterrows():
                    if symbol in self.symbols:  # 确保是我们要处理的 symbol
                        self._handle_signal(symbol, signal, current_ts)

            # 4. 更新投资组合价值和权益曲线
            self._update_portfolio(current_ts)

            # 检查是否爆仓
            if self.equity <= 0:
                logger.error(
                    f"[{current_ts}] 账户权益耗尽 ({self.equity:.2f})！回测提前终止。"
                )
                break  # 提前结束循环

        end_run_time = time.time()
        logger.info(f"回测事件循环完成。耗时: {end_run_time - start_run_time:.2f} 秒。")

    def _handle_signal(self, symbol: str, signal: pd.Series, current_ts: pd.Timestamp):
        """(同步) 处理单个交易信号，模拟订单执行。"""
        action = signal.get("action")  # 'buy', 'sell', 'close'
        signal_price = signal.get("price")  # 信号触发时的价格 (通常是 K 线收盘价)
        reason = signal.get("reason", "N/A")
        position_size_signal = signal.get(
            "position_size", self.position_sizing_pct
        )  # 允许信号指定大小，否则用默认

        if not action:
            return

        # 获取当前市场价格用于模拟成交 (使用当前 K 线的收盘价)
        try:
            current_bar = self.all_market_data.loc[(current_ts, symbol), :]
            # 检查数据类型，确保是 Series 或类似结构
            if not hasattr(current_bar, "loc") or "close" not in current_bar:
                raise KeyError(
                    f"无法从 current_bar 获取 'close' 价格: {type(current_bar)}"
                )
            execution_price = float(current_bar["close"])  # 模拟在此 K 线的收盘价成交
            if execution_price <= 0:
                logger.warning(
                    f"[{current_ts}|{symbol}] 当前收盘价无效 ({execution_price})，无法执行信号 {action}。"
                )
                return
        except KeyError:
            logger.warning(
                f"[{current_ts}|{symbol}] 无法找到当前 K 线数据，跳过信号 {action}。"
            )
            return
        except Exception as price_e:
            logger.error(
                f"[{current_ts}|{symbol}] 获取执行价格时出错: {price_e}", exc_info=True
            )
            return

        # 应用滑点
        buy_slippage = execution_price * self.slippage_pct
        sell_slippage = execution_price * self.slippage_pct
        # 买入价更高，卖出价更低
        adjusted_buy_price = round(execution_price + buy_slippage, self.price_precision)
        adjusted_sell_price = round(
            execution_price - sell_slippage, self.price_precision
        )

        # 获取当前持仓
        current_position = self.positions.get(symbol)
        current_side = current_position["side"] if current_position else None
        current_amount = current_position["amount"] if current_position else 0.0
        entry_price = current_position["entry_price"] if current_position else 0.0

        trade_executed = False
        pnl = 0.0
        commission = 0.0
        trade_details = {
            "timestamp": current_ts,
            "symbol": symbol,
            "action": action,  # 'buy', 'sell', 'close'
            "signal_price": signal_price,
            "reason": reason,
        }

        # --- 处理开仓/反手 ---
        if action == "buy":  # 开多或平空开多
            target_price = adjusted_buy_price  # 开多/平空用买价
            if current_side == "short":  # 先平空
                logger.debug(f"[{current_ts}|{symbol}] 信号: BUY (反手). 平空仓...")
                close_price = target_price
                pnl = (entry_price - close_price) * current_amount
                cost_or_proceeds = close_price * current_amount  # 平仓成本
                commission = cost_or_proceeds * self.commission_rate
                self.cash += pnl - commission
                self.trades.append(
                    {
                        **trade_details,
                        "order_type": "close_short",
                        "price": close_price,
                        "amount": current_amount,
                        "commission": commission,
                        "pnl": pnl,
                        "cash": self.cash,
                    }
                )
                logger.debug(
                    f"[{current_ts}|{symbol}] 空仓已平. PnL: {pnl:.4f}, Comm: {commission:.4f}, Cash: {self.cash:.4f}"
                )
                del self.positions[symbol]
                current_side = None
                current_amount = 0.0  # 重置以便后续开多

            if current_side != "long":  # 开多仓 (已排除已是多头的情况)
                logger.debug(
                    f"[{current_ts}|{symbol}] 信号: BUY. 开多仓 (Reason: {reason})..."
                )
                # 计算仓位大小
                nominal_value_target = (
                    self.equity * position_size_signal * self.leverage
                )
                if nominal_value_target <= 0:
                    logger.warning(
                        f"[{current_ts}|{symbol}] 目标名义价值 <= 0 ({nominal_value_target:.2f})，无法开仓。权益: {self.equity:.2f}"
                    )
                    return
                amount_to_open = nominal_value_target / target_price

                # 应用精度调整 (使用 round)
                amount_to_open = round(amount_to_open, self.amount_precision)

                if amount_to_open * target_price < 1:  # 检查最小名义价值 (例如 $1)
                    logger.warning(
                        f"[{current_ts}|{symbol}] 计算得到的开仓名义价值过小 ({amount_to_open * target_price:.4f})，无法开仓。"
                    )
                    return
                if amount_to_open <= 0:
                    logger.warning(
                        f"[{current_ts}|{symbol}] 计算得到的开仓量为 0 或负数，无法开仓。"
                    )
                    return

                cost = target_price * amount_to_open
                commission = cost * self.commission_rate
                # 检查是否有足够现金支付手续费 (简化：不严格检查，假设手续费能支付)
                self.cash -= commission  # 开仓只扣手续费
                self.positions[symbol] = {
                    "side": "long",
                    "amount": amount_to_open,
                    "entry_price": target_price,
                    "value": cost,  # 记录开仓时的名义价值
                    "entry_ts": current_ts,  # 记录入场时间
                }
                trade_executed = True
                self.trades.append(
                    {
                        **trade_details,
                        "order_type": "open_long",
                        "price": target_price,
                        "amount": amount_to_open,
                        "commission": commission,
                        "pnl": 0.0,  # 开仓 PnL 为 0
                        "cash": self.cash,
                    }
                )
                logger.debug(
                    f"[{current_ts}|{symbol}] 多仓已开. Amount: {amount_to_open:.6f}, Price: {target_price:.4f}, Comm: {commission:.4f}, Cash: {self.cash:.4f}"
                )
            else:
                logger.debug(f"[{current_ts}|{symbol}] 信号: BUY, 但已持有多仓，忽略。")

        elif action == "sell":  # 开空或平多开空
            target_price = adjusted_sell_price  # 开空/平多用卖价
            if current_side == "long":  # 先平多
                logger.debug(f"[{current_ts}|{symbol}] 信号: SELL (反手). 平多仓...")
                close_price = target_price
                pnl = (close_price - entry_price) * current_amount
                cost_or_proceeds = close_price * current_amount  # 平仓收入
                commission = cost_or_proceeds * self.commission_rate
                self.cash += pnl - commission
                self.trades.append(
                    {
                        **trade_details,
                        "order_type": "close_long",
                        "price": close_price,
                        "amount": current_amount,
                        "commission": commission,
                        "pnl": pnl,
                        "cash": self.cash,
                    }
                )
                logger.debug(
                    f"[{current_ts}|{symbol}] 多仓已平. PnL: {pnl:.4f}, Comm: {commission:.4f}, Cash: {self.cash:.4f}"
                )
                del self.positions[symbol]
                current_side = None
                current_amount = 0.0

            if current_side != "short":  # 开空仓
                logger.debug(
                    f"[{current_ts}|{symbol}] 信号: SELL. 开空仓 (Reason: {reason})..."
                )
                # 计算仓位大小 (同开多逻辑)
                nominal_value_target = (
                    self.equity * position_size_signal * self.leverage
                )
                if nominal_value_target <= 0:
                    logger.warning(
                        f"[{current_ts}|{symbol}] 目标名义价值 <= 0 ({nominal_value_target:.2f})，无法开仓。权益: {self.equity:.2f}"
                    )
                    return
                amount_to_open = nominal_value_target / target_price

                amount_to_open = round(amount_to_open, self.amount_precision)

                if amount_to_open * target_price < 1:
                    logger.warning(
                        f"[{current_ts}|{symbol}] 计算得到的开仓名义价值过小 ({amount_to_open * target_price:.4f})，无法开仓。"
                    )
                    return
                if amount_to_open <= 0:
                    logger.warning(
                        f"[{current_ts}|{symbol}] 计算得到的开仓量为 0 或负数，无法开仓。"
                    )
                    return

                cost = target_price * amount_to_open  # 空头成本也是用价值算
                commission = cost * self.commission_rate
                self.cash -= commission
                self.positions[symbol] = {
                    "side": "short",
                    "amount": amount_to_open,
                    "entry_price": target_price,
                    "value": cost,
                    "entry_ts": current_ts,
                }
                trade_executed = True
                self.trades.append(
                    {
                        **trade_details,
                        "order_type": "open_short",
                        "price": target_price,
                        "amount": amount_to_open,
                        "commission": commission,
                        "pnl": 0.0,
                        "cash": self.cash,
                    }
                )
                logger.debug(
                    f"[{current_ts}|{symbol}] 空仓已开. Amount: {amount_to_open:.6f}, Price: {target_price:.4f}, Comm: {commission:.4f}, Cash: {self.cash:.4f}"
                )
            else:
                logger.debug(
                    f"[{current_ts}|{symbol}] 信号: SELL, 但已持有空仓，忽略。"
                )

        # --- 处理平仓 ---
        elif action == "close":
            if current_side == "long":
                logger.debug(
                    f"[{current_ts}|{symbol}] 信号: CLOSE. 平多仓 (Reason: {reason})..."
                )
                close_price = adjusted_sell_price  # 平多用卖价
                pnl = (close_price - entry_price) * current_amount
                cost_or_proceeds = close_price * current_amount
                commission = cost_or_proceeds * self.commission_rate
                self.cash += pnl - commission
                trade_executed = True
                self.trades.append(
                    {
                        **trade_details,
                        "order_type": "close_long",
                        "price": close_price,
                        "amount": current_amount,
                        "commission": commission,
                        "pnl": pnl,
                        "cash": self.cash,
                    }
                )
                logger.debug(
                    f"[{current_ts}|{symbol}] 多仓已平. PnL: {pnl:.4f}, Comm: {commission:.4f}, Cash: {self.cash:.4f}"
                )
                del self.positions[symbol]

            elif current_side == "short":
                logger.debug(
                    f"[{current_ts}|{symbol}] 信号: CLOSE. 平空仓 (Reason: {reason})..."
                )
                close_price = adjusted_buy_price  # 平空用买价
                pnl = (entry_price - close_price) * current_amount
                cost_or_proceeds = close_price * current_amount
                commission = cost_or_proceeds * self.commission_rate
                self.cash += pnl - commission
                trade_executed = True
                self.trades.append(
                    {
                        **trade_details,
                        "order_type": "close_short",
                        "price": close_price,
                        "amount": current_amount,
                        "commission": commission,
                        "pnl": pnl,
                        "cash": self.cash,
                    }
                )
                logger.debug(
                    f"[{current_ts}|{symbol}] 空仓已平. PnL: {pnl:.4f}, Comm: {commission:.4f}, Cash: {self.cash:.4f}"
                )
                del self.positions[symbol]
            else:
                logger.debug(f"[{current_ts}|{symbol}] 信号: CLOSE, 但无持仓。")
        else:
            logger.warning(f"[{current_ts}|{symbol}] 未知信号动作: {action}")

    def _update_portfolio(self, current_ts: pd.Timestamp):
        """(同步) 在每个时间点结束时更新投资组合的总权益。"""
        total_unrealized_pnl = 0.0
        position_value_total = 0.0  # 总持仓名义价值

        # 获取当前时间点所有 relevant symbol 的收盘价
        try:
            # .loc[current_ts] 可能返回 Series (如果只有一个 symbol) 或 DataFrame
            latest_data_at_ts = self.all_market_data.loc[current_ts]

            # 定义一个辅助函数来获取价格
            def get_price(symbol, data):
                if isinstance(data, pd.Series):  # 如果只有一个 symbol 的数据
                    if data.name == symbol and "close" in data:  # 检查 name 是否匹配
                        return float(data["close"])
                    else:  # 可能没匹配上或列不存在
                        return None
                elif isinstance(
                    data, pd.DataFrame
                ):  # 如果返回了多个 symbol 的 DataFrame
                    if symbol in data.index and "close" in data.columns:
                        return float(data.loc[symbol, "close"])
                    else:
                        return None
                else:  # 其他意外类型
                    return None

            # 计算每个持仓的未实现盈亏
            for symbol, position in self.positions.items():
                current_price = get_price(symbol, latest_data_at_ts)

                if current_price is None or current_price <= 0:
                    logger.warning(
                        f"[{current_ts}|{symbol}] 更新投资组合时无法获取有效价格，PnL 计算可能不准确。"
                    )
                    # 无法计算 PnL，可以假设 PnL 不变或使用上一个价格（简化：假设 PnL 为 0）
                    unrealized_pnl = position.get(
                        "last_unrealized_pnl", 0.0
                    )  # 保留下上次的值
                else:
                    amount = position["amount"]
                    entry_price = position["entry_price"]
                    side = position["side"]
                    unrealized_pnl = 0.0
                    if side == "long":
                        unrealized_pnl = (current_price - entry_price) * amount
                    elif side == "short":
                        unrealized_pnl = (entry_price - current_price) * amount
                    position["last_unrealized_pnl"] = unrealized_pnl  # 存储 PnL

                    # 更新总持仓名义价值
                    position_value_total += current_price * amount

                total_unrealized_pnl += unrealized_pnl

        except KeyError:
            # 如果 current_ts 在 all_market_data.index 中不存在
            logger.warning(
                f"[{current_ts}] 更新投资组合时在 all_market_data 中找不到数据，权益将保持上一个值。"
            )
            if self.equity_curve:
                self.equity = self.equity_curve[max(self.equity_curve.keys())]
            else:
                self.equity = self.cash
            self.equity_curve[current_ts] = self.equity
            return  # 直接返回，不进行后续计算

        except Exception as port_e:
            logger.error(
                f"[{current_ts}] 更新投资组合时发生未知错误: {port_e}", exc_info=True
            )
            # 同样保持上一个权益值
            if self.equity_curve:
                self.equity = self.equity_curve[max(self.equity_curve.keys())]
            else:
                self.equity = self.cash
            self.equity_curve[current_ts] = self.equity
            return

        # 更新总权益 = 现金 + 总未实现盈亏
        self.equity = self.cash + total_unrealized_pnl
        self.equity_curve[current_ts] = self.equity
        # logger.debug(f"[{current_ts}] Portfolio Updated. Cash: {self.cash:.2f}, UPL: {total_unrealized_pnl:.2f}, Equity: {self.equity:.2f}")

    def _calculate_results(self) -> Dict[str, Any]:
        """(同步) 计算最终的回测结果和指标。"""
        logger.info("计算回测结果...")
        if not self.equity_curve:
            logger.error("权益曲线为空，无法计算结果。")
            return {"error": "Equity curve is empty"}

        try:
            equity_series = pd.Series(self.equity_curve).sort_index()
            # 移除第一个用于初始化的点
            if not equity_series.empty:
                first_real_event_ts = self.event_timestamps[0]
                # 保留第一个真实事件点之后（包括该点）的数据
                equity_series = equity_series[
                    equity_series.index >= first_real_event_ts
                ]

            if equity_series.empty:
                logger.error("处理后的权益曲线为空。")
                return {"error": "Processed equity curve is empty"}

            trades_df = pd.DataFrame(self.trades)
            if not trades_df.empty:
                trades_df["timestamp"] = pd.to_datetime(trades_df["timestamp"])
                trades_df = trades_df.set_index("timestamp").sort_index()

            # 使用 results.py 中的函数计算指标
            metrics = calculate_metrics(equity_series, trades_df, self.initial_capital)

            # 准备返回结果
            results = {
                "metrics": metrics,
                "equity_curve": equity_series,
                "trades": trades_df,
            }
            return results
        except Exception as calc_e:
            logger.error(f"计算结果时出错: {calc_e}", exc_info=True)
            return {"error": f"结果计算失败: {calc_e}"}

    async def run(self) -> Dict[str, Any]:
        """
        (异步) 运行完整的回测流程。
        包括数据准备、事件循环和结果计算。
        """
        results = {}
        try:
            await self._prepare_data()  # 异步准备数据
            if self.all_market_data is None or self.event_timestamps is None:
                raise RuntimeError("数据准备失败，无法继续回测。")
            self._run_event_loop()  # 同步运行事件循环
            results = self._calculate_results()  # 同步计算结果
            return results
        except Exception as e:
            logger.error(
                f"回测过程中发生严重错误: {e}", exc_info=True
            )  # 使用 exc_info=True 打印堆栈
            # logger.error(traceback.format_exc()) # 不需要重复打印
            results = {"error": str(e)}  # 返回错误信息
            # 即使出错，也尝试返回已有的部分结果，以便调试
            results["equity_curve"] = pd.Series(self.equity_curve).sort_index()
            results["trades"] = pd.DataFrame(self.trades)
            if not results["trades"].empty:
                results["trades"]["timestamp"] = pd.to_datetime(
                    results["trades"]["timestamp"]
                )
                results["trades"] = (
                    results["trades"].set_index("timestamp").sort_index()
                )
            return results
        finally:
            # 关闭数据库连接
            if self.db:
                try:
                    self.db.close()
                except Exception as db_close_e:
                    logger.warning(f"关闭数据库连接时出错: {db_close_e}")
            # 关闭交易所接口 (如果创建了)
            if self.exchange_interface:
                try:
                    # 确保异步关闭被 await
                    await self.exchange_interface.close()
                except Exception as close_e:
                    logger.warning(f"关闭交易所接口时出错: {close_e}")
