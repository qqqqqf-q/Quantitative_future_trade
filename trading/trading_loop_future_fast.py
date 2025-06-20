import time
import datetime
import pandas as pd
import asyncio
import json
import traceback
from typing import Dict, Optional, Any, List
import numpy as np

# 使用相对导入或确保 PYTHONPATH 正确 (假设在 future_trade 目录下运行)
from trading.api import ExchangeInterface  # 同级导入
from utils.logger import logger
from database.sqlite import SQLiteDB
from strategies.mystrategy import MyStrategy


class TradingLoop:
    """
    (异步重构版)
    主交易循环，负责协调数据获取、信号生成、订单执行和状态记录。
    完全基于 asyncio 运行。
    """

    def __init__(
        self,
        strategy: MyStrategy,
        exchange_interface: ExchangeInterface,
        db_instance: SQLiteDB,
    ):
        self.strategy = strategy
        self.exchange_interface = exchange_interface
        self.db = db_instance
        self.logger = logger

        # --- 运行时参数 ---
        self.current_task_id: Optional[int] = None
        self.task_name: Optional[str] = None
        self.symbols: List[str] = []
        self.leverage: Optional[int] = None
        self.position_size: Optional[float] = None
        self.signal_interval: float = 1.0
        self.price_check_interval: float = 0.2
        self.kline_limit: int = 1000
        self.kline_interval_minutes: int = 1
        self.kline_interval_str: str = "1m"

        # --- 内部状态 ---
        self.positions: Dict[str, Dict] = {}
        self.latest_tickers: Dict[str, Dict] = {}
        self.signals: Optional[pd.DataFrame] = None
        self.last_signal_time: float = 0
        self.last_account_log_time: float = 0
        self.account_log_interval: int = 60

        # --- Asyncio 相关 ---
        self._stop_requested = False
        self._tasks: List[asyncio.Task] = []  # 存储 asyncio 任务

    def _load_task_parameters(self):
        """从数据库加载并设置任务参数 (同步方法)"""
        if self.current_task_id is None:
            raise ValueError("无法加载参数，current_task_id 未设置")

        task_info = self.db.get_task(self.current_task_id)
        if not task_info:
            raise ValueError(f"无法获取 Task ID: {self.current_task_id} 的信息")

        self.task_name = task_info.get("task_name", f"Task_{self.current_task_id}")
        parameters_json = task_info.get("parameters", "{}")
        try:
            params = (
                json.loads(parameters_json)
                if isinstance(parameters_json, str)
                else parameters_json
            )
            if not isinstance(params, dict):
                raise ValueError("任务参数格式不正确，应为 JSON 对象")

            self.signal_interval = float(params.get("signal_interval", 1.0))
            self.price_check_interval = float(params.get("price_check_interval", 0.2))
            self.leverage = int(params.get("leverage", 10))
            self.symbols = list(params.get("symbols", []))
            self.kline_limit = int(params.get("kline_limit", 1000))
            self.kline_interval_minutes = int(params.get("kline_interval_minutes", 1))
            self.position_size = float(params.get("position_size", 0.1))

            if not self.symbols:
                raise ValueError("任务参数中必须包含 'symbols' 列表")
            if not (0 < self.position_size <= 1):
                raise ValueError(
                    f"无效的 'position_size': {self.position_size}，必须在 (0, 1] 之间"
                )

            self.kline_interval_str = f"{self.kline_interval_minutes}m"

            logger.info(f"[Task {self.current_task_id}] 任务参数加载成功:")
            logger.info(f"  Symbols: {self.symbols}")
            logger.info(
                f"  Leverage: {self.leverage}x, Position Size: {self.position_size*100:.2f}%"
            )
            logger.info(
                f"  Signal Interval: {self.signal_interval}s, Price Check Interval: {self.price_check_interval}s"
            )
            logger.info(
                f"  Kline: {self.kline_interval_str}, Limit: {self.kline_limit}"
            )

        except (json.JSONDecodeError, ValueError, TypeError) as e:
            logger.error(f"解析任务 {self.current_task_id} 参数失败: {e}")
            raise ValueError(f"任务参数解析失败: {e}")

    # --- 后台任务 (Async Tasks) ---

    async def _kline_fetcher_task(self):
        """(异步任务) 定期获取 K 线数据并存入数据库"""
        task_name = "KlineFetcherTask"
        logger.info(
            f"[{task_name}] 启动，目标周期: {self.kline_interval_str}, 数量: {self.kline_limit}"
        )
        # K线获取间隔稍微小于周期，保证能拿到最新K线，但也别太频繁
        fetch_interval_seconds = max(
            30, self.kline_interval_minutes * 60 * 0.95
        )  # 最快30秒获取一次

        while not self._stop_requested:
            start_time = time.time()
            all_fetched_ok = True
            try:
                # logger.debug(f"[{task_name}] 开始本轮 K 线获取...")
                # 并发获取所有 symbols 的 K 线
                fetch_jobs = [
                    self.exchange_interface.fetch_ohlcv(
                        symbol,
                        timeframe=self.kline_interval_str,
                        limit=self.kline_limit + 5,  # 多获取一点以防万一
                    )
                    for symbol in self.symbols
                ]
                results = await asyncio.gather(*fetch_jobs, return_exceptions=True)

                for symbol, result in zip(self.symbols, results):
                    if self._stop_requested:
                        break
                    if isinstance(result, Exception):
                        logger.error(
                            f"[{task_name}|{symbol}] 获取 K 线时出错: {result}"
                        )
                        all_fetched_ok = False
                        continue
                    elif isinstance(result, pd.DataFrame) and not result.empty:
                        # 获取成功，处理并存储
                        try:
                            df_klines = result
                            df_reset = df_klines.reset_index()
                            df_reset["timestamp"] = (
                                df_reset["timestamp"].astype(np.int64) // 10**6
                            )
                            klines_list = df_reset[
                                ["timestamp", "open", "high", "low", "close", "volume"]
                            ].values.tolist()

                            # --- 数据库操作是同步的 ---
                            insert_ok = self.db.insert_klines(
                                symbol, self.kline_interval_str, klines_list
                            )
                            if insert_ok:
                                maintain_ok = self.db.maintain_klines_limit(
                                    symbol, self.kline_interval_str, self.kline_limit
                                )
                                if not maintain_ok:
                                    logger.warning(
                                        f"[{task_name}|{symbol}] maintain_klines_limit 失败"
                                    )
                                # logger.debug(f"[{task_name}|{symbol}] K 线数据已处理并存入数据库")
                            else:
                                logger.warning(
                                    f"[{task_name}|{symbol}] insert_klines 失败"
                                )
                                all_fetched_ok = False
                        except Exception as process_e:
                            logger.error(
                                f"[{task_name}|{symbol}] 处理或存储 K 线出错: {process_e}"
                            )
                            logger.error(traceback.format_exc())
                            all_fetched_ok = False
                    else:
                        # logger.warning(f"[{task_name}|{symbol}] 未获取到有效 K 线 DataFrame")
                        all_fetched_ok = False

                    if not self._stop_requested:
                        await asyncio.sleep(0.1)  # 防止单个 symbol 处理过快

            except asyncio.CancelledError:
                logger.info(f"[{task_name}] 被取消")
                break
            except Exception as e:
                logger.error(f"[{task_name}] 循环内部出错: {e}")
                logger.error(traceback.format_exc())
                all_fetched_ok = False
                if not self._stop_requested:
                    await asyncio.sleep(60)  # 出错后等待更长时间

            finally:
                elapsed = time.time() - start_time
                next_wait = (
                    fetch_interval_seconds
                    if all_fetched_ok
                    else max(fetch_interval_seconds, 120)
                )  # 失败则等待更久
                sleep_time = max(0, next_wait - elapsed)
                if not self._stop_requested:
                    try:
                        # logger.debug(f"[{task_name}] K线获取本轮耗时 {elapsed:.2f}s, 等待 {sleep_time:.2f}s")
                        await asyncio.sleep(sleep_time)
                    except asyncio.CancelledError:
                        logger.info(f"[{task_name}] 在 sleep 时被取消")
                        break
        logger.info(f"[{task_name}] 停止")

    async def _websocket_ticker_task(self):
        """(异步任务) 监听 Ticker 数据 (适配返回 dict 的 watch_tickers)"""
        task_name = "WebSocketTickerTask"
        logger.info(f"[{task_name}] 启动，监听 Tickers: {self.symbols}")
        try:
            # --- 不再使用 async for ---
            # 调用 watch_tickers() 返回的是一个异步生成器
            ticker_generator = self.exchange_interface.watch_tickers(self.symbols)

            while not self._stop_requested:
                try:
                    # --- 从生成器获取下一个 Ticker 字典 ---
                    # 注意：如果 watch_tickers 内部 await 阻塞，这里也会阻塞
                    ticker = await ticker_generator.asend(
                        None
                    )  # 或者 ticker_generator.__anext__()

                    if self._stop_requested:
                        break

                    # --- 处理收到的 ticker 字典 ---
                    if ticker:  # 确保收到了数据
                        symbol = ticker.get("symbol")
                        if symbol and symbol in self.symbols:
                            price = ticker.get("last")
                            if price is None:
                                bid = ticker.get("bid")
                                ask = ticker.get("ask")
                                if bid is not None and ask is not None:
                                    price = (bid + ask) / 2.0
                            if (
                                price is not None
                                and ticker.get("timestamp") is not None
                            ):
                                self.latest_tickers[symbol] = {
                                    "timestamp": ticker["timestamp"],
                                    "price": float(price),
                                }
                                logger.debug(
                                    f"[{task_name}] Updated ticker for {symbol}: Price={price}"
                                )  # <-- 添加Debug日志确认更新

                except StopAsyncIteration:
                    logger.warning(
                        f"[{task_name}] Ticker generator finished unexpectedly. Restarting..."
                    )
                    await asyncio.sleep(5)  # 等待后重新获取生成器
                    ticker_generator = self.exchange_interface.watch_tickers(
                        self.symbols
                    )
                    continue  # 继续下一次循环
                except asyncio.CancelledError:
                    logger.info(f"[{task_name}] 被取消")
                    break
                except Exception as e:
                    logger.error(f"[{task_name}] 处理 ticker 时出错: {e}")
                    logger.error(traceback.format_exc())
                    # 出错后可以等待一下，依赖 watch_tickers 内部的重连
                    await asyncio.sleep(5)

        except asyncio.CancelledError:
            logger.info(f"[{task_name}] 启动时被取消")
        except Exception as task_e:
            logger.error(f"[{task_name}] 任务启动或运行时发生严重错误: {task_e}")
            logger.error(traceback.format_exc())

        logger.info(f"[{task_name}] 停止")

    async def _set_leverage_task(self):
        """(异步任务) 设置杠杆"""
        task_name = "SetLeverageTask"
        if self.leverage is None:
            logger.info(f"[{task_name}] 未指定杠杆，跳过")
            return True
        logger.info(f"[{task_name}] 开始为 {self.symbols} 设置杠杆为 {self.leverage}x")
        try:
            results = await asyncio.gather(
                *[
                    self.exchange_interface.set_leverage(symbol, self.leverage)
                    for symbol in self.symbols
                ],
                return_exceptions=True,
            )
            all_set_ok = True
            for symbol, result in zip(self.symbols, results):
                if isinstance(result, Exception):
                    logger.error(f"[{task_name}] 为 {symbol} 设置杠杆出错: {result}")
                    all_set_ok = False
                elif result:
                    logger.info(
                        f"[{task_name}] 尝试为 {symbol} 设置杠杆返回: {result}"
                    )  # 打印结果看是否成功
                else:
                    logger.warning(
                        f"[{task_name}] 为 {symbol} 设置杠杆调用返回 None 或 False"
                    )
                    # 即使返回 None 也可能意味着杠杆已是目标值，不一定算失败
                    # all_set_ok = False # 可以选择是否将 None 视为失败

            if all_set_ok:
                logger.info(f"[{task_name}] 杠杆设置完成 (或尝试完成)")
            else:
                logger.warning(f"[{task_name}] 部分杠杆设置可能未成功")
            return all_set_ok
        except Exception as e:
            logger.error(f"[{task_name}] 设置杠杆时发生意外错误: {e}")
            logger.error(traceback.format_exc())
            return False

    # --- 主循环逻辑 (Async) ---
    async def run(self, task_id: int):
        """启动并运行异步交易循环"""
        self.current_task_id = task_id
        logger.info(f"任务 {task_id} 开始异步运行...")
        try:
            self._load_task_parameters()
            # --- 异步初始化交易所 ---
            await self.exchange_interface.initialize()
            if not self.db.update_task_status(task_id=task_id, status="running"):
                raise RuntimeError("无法更新任务状态为 'running'")

            # --- 启动并发任务 ---
            self._stop_requested = False
            self._tasks = []

            # 启动设置杠杆任务 (等待完成)
            leverage_ok = await self._set_leverage_task()
            # 可以根据 leverage_ok 决定是否继续
            # if not leverage_ok: logger.warning("杠杆设置失败，请检查配置或API密钥权限")

            # 启动 K 线获取任务
            kline_task = asyncio.create_task(
                self._kline_fetcher_task(), name="KlineFetcherTask"
            )
            self._tasks.append(kline_task)

            # 启动 Ticker 监听任务
            ticker_task = asyncio.create_task(
                self._websocket_ticker_task(), name="WebSocketTickerTask"
            )
            self._tasks.append(ticker_task)

            # --- 初始化主循环状态 ---
            logger.info("等待后台任务初始化 (例如 WebSocket 连接)... (等待 5 秒)")
            await asyncio.sleep(5)
            logger.info("获取初始持仓信息...")
            await self.update_positions()
            logger.info("生成初始交易信号...")
            await self._generate_signals()
            self.last_signal_time = time.time()
            self.last_account_log_time = time.time()
            logger.info(f"[Task {task_id}] 初始化完成，进入主异步循环...")

            # --- 主异步循环 ---
            while not self._stop_requested:
                current_db_status = self.db.get_task_status(task_id)
                if current_db_status in ["stopping", "stop"]:
                    logger.info(
                        f"任务 {task_id} 收到数据库停止信号 ({current_db_status})"
                    )
                    self._stop_requested = True
                    break
                current_time = time.time()

                # a. 定期生成交易信号
                if current_time - self.last_signal_time >= self.signal_interval:
                    await self._generate_signals()
                    self.last_signal_time = current_time

                # b. 执行交易
                if self.signals is not None and not self.signals.empty:
                    await self.execute_trades_based_on_signals()

                # d. 记录账户状态
                if (
                    current_time - self.last_account_log_time
                    >= self.account_log_interval
                ):
                    await self.log_account_status()
                    self.last_account_log_time = current_time

                # e. 异步休眠
                try:
                    await asyncio.sleep(self.price_check_interval)
                except asyncio.CancelledError:
                    logger.info("主循环 sleep 被取消")
                    self._stop_requested = True
                    break

            logger.info(f"任务 {task_id} 主循环结束")

        except Exception as e:
            logger.error(f"任务 {task_id} 交易主循环发生严重错误: {e}")
            logger.error(traceback.format_exc())
            # 尝试更新数据库状态
            try:
                self.db.update_task_status(
                    task_id, "error", result=f"Main loop error: {e}"
                )
            except Exception as db_e:
                logger.error(f"更新任务状态为 error 时失败: {db_e}")
        finally:
            await self._shutdown()

    async def _shutdown(self):
        """(异步) 执行停止流程"""
        task_id = self.current_task_id
        logger.info(f"任务 {task_id} 开始执行异步停止流程...")
        self._stop_requested = True
        logger.info("正在取消后台 asyncio 任务...")
        cancelled_tasks = []
        for task in self._tasks:  # 现在只包含 ticker_task
            if task and not task.done():
                task.cancel()
                cancelled_tasks.append(task)
        if cancelled_tasks:
            results = await asyncio.gather(*cancelled_tasks, return_exceptions=True)
            logger.debug(f"后台任务取消结果: {results}")
        logger.info("后台 asyncio 任务已取消/完成")
        await self.exchange_interface.close()

        # 3. 更新最终任务状态
        final_status = "unknown"
        try:
            current_db_status = self.db.get_task_status(task_id)
            logger.info(f"任务 {task_id} 当前数据库状态: {current_db_status}")

            if current_db_status == "error":
                final_status = "error"
            elif current_db_status in ["stopping", "stop"]:
                final_status = "stop"
            elif current_db_status == "running":
                # 如果是因为错误退出主循环，状态应为 error
                # 这里需要一种方式判断退出原因，暂时简化
                final_status = "completed"  # 假设正常停止是 completed
            else:
                final_status = current_db_status or "completed"

            if current_db_status != final_status and current_db_status not in [
                "error",
                "stop",
                "completed",
            ]:
                logger.info(f"任务 {task_id} 最终状态确定为: {final_status}")
                self.db.update_task_status(
                    task_id,
                    final_status,
                    result=f"Async loop finished with status {final_status}",
                )
            else:
                logger.info(f"任务 {task_id} 最终状态保持为: {final_status}")
        except Exception as db_e:
            logger.error(f"更新最终任务状态时数据库出错: {db_e}")

        logger.info(f"任务 {task_id} 异步停止流程执行完毕")

    # --- 核心逻辑方法 (Async) ---

    async def _get_market_data(self) -> Optional[Dict[str, pd.DataFrame]]:
        """(异步) 获取市场数据，并在获取后尝试存入数据库"""
        all_symbol_data = {}

        async def fetch_and_process(symbol):
            try:
                # --- 直接从 API 获取 K 线 ---
                df_klines_api = await self.exchange_interface.fetch_ohlcv(
                    symbol,
                    timeframe=self.kline_interval_str,
                    limit=self.kline_limit + 5,
                )
                if df_klines_api.empty:
                    logger.warning(f"[{symbol}] 从 API 获取 K 线失败，尝试 DB")
                    df_klines_db = self.db.get_klines(
                        symbol, self.kline_interval_str, self.kline_limit
                    )
                    if df_klines_db.empty:
                        logger.error(f"[{symbol}] 无法从 API 和 DB 获取 K 线")
                        return symbol, None
                    else:
                        logger.info(f"[{symbol}] 使用了数据库 K 线")
                        df_klines_api = df_klines_db  # 使用 DB 数据

                # --- 尝试存入数据库 (同步) ---
                try:
                    df_reset = df_klines_api.reset_index()
                    df_reset["timestamp"] = (
                        df_reset["timestamp"].astype(np.int64) // 10**6
                    )
                    klines_list = df_reset[
                        ["timestamp", "open", "high", "low", "close", "volume"]
                    ].values.tolist()
                    insert_ok = self.db.insert_klines(
                        symbol, self.kline_interval_str, klines_list
                    )
                    if insert_ok:
                        maintain_ok = self.db.maintain_klines_limit(
                            symbol, self.kline_interval_str, self.kline_limit
                        )
                        if maintain_ok:
                            logger.debug(f"[{symbol}] K线数据已存库并维护")
                    # else: logger.warning(...)
                except Exception as db_e:
                    logger.error(f"[{symbol}] 存 K 线失败: {db_e}")

                # --- Ticker 融合 ---
                df_merged = df_klines_api.copy()
                latest_ticker = self.latest_tickers.get(symbol)
                if latest_ticker and not df_merged.empty:
                    # ... (融合逻辑不变) ...
                    ticker_price = latest_ticker["price"]
                    ticker_timestamp_ms = latest_ticker["timestamp"]
                    last_kline_time = df_merged.index[-1]
                    last_kline_ts_ms = int(last_kline_time.timestamp() * 1000)
                    if ticker_timestamp_ms > last_kline_ts_ms:
                        # ... (更新或创建 K 线) ...
                        kline_interval_ms = self.kline_interval_minutes * 60 * 1000
                        current_kline_start_ts_ms = (
                            ticker_timestamp_ms // kline_interval_ms
                        ) * kline_interval_ms
                        current_kline_start_dt = pd.to_datetime(
                            current_kline_start_ts_ms, unit="ms", utc=True
                        )
                        if current_kline_start_dt == last_kline_time:
                            df_merged.loc[last_kline_time, "close"] = ticker_price
                            df_merged.loc[last_kline_time, "high"] = max(
                                df_merged.loc[last_kline_time, "high"], ticker_price
                            )
                            df_merged.loc[last_kline_time, "low"] = min(
                                df_merged.loc[last_kline_time, "low"], ticker_price
                            )
                        elif current_kline_start_dt > last_kline_time:
                            # ... (创建新 K 线并合并) ...
                            prev_close = df_merged.loc[last_kline_time, "close"]
                            new_kline_data = {
                                "open": prev_close,
                                "high": max(prev_close, ticker_price),
                                "low": min(prev_close, ticker_price),
                                "close": ticker_price,
                                "volume": 0,
                            }
                            new_kline = pd.DataFrame(
                                [new_kline_data], index=[current_kline_start_dt]
                            ).astype(df_merged.dtypes)
                            df_merged = pd.concat([df_merged, new_kline])
                            if len(df_merged) > self.kline_limit:
                                df_merged = df_merged.iloc[-(self.kline_limit) :]

                return symbol, df_merged
            except Exception as e:  # ...
                return symbol, None

        results = await asyncio.gather(*(fetch_and_process(s) for s in self.symbols))
        for symbol, df_data in results:
            if df_data is not None:
                all_symbol_data[symbol] = df_data
        if not all_symbol_data:
            return None
        return all_symbol_data

    async def _generate_signals(self):
        """(异步) 获取数据并调用策略生成信号"""
        # logger.debug("开始生成信号...")
        market_data = await self._get_market_data()
        if market_data is None or not market_data:
            # logger.warning("无市场数据，无法生成信号")
            self.signals = None
            return
        try:
            # 策略生成本身是同步的
            # 如果策略计算量大，可以考虑 loop.run_in_executor
            self.signals = self.strategy.generate_signals(market_data)
            # if self.signals is not None and not self.signals.empty: logger.info(f"生成 {len(self.signals)} 条信号")
            # else: logger.debug("未生成有效信号")
        except Exception as e:
            logger.error(f"策略 {self.strategy.name} 生成信号失败: {e}")
            logger.error(traceback.format_exc())
            self.signals = None

    async def update_positions(self):
        """(异步) 从交易所同步更新当前持仓信息"""
        try:
            # logger.debug("正在更新持仓信息...")
            current_positions_list = await self.exchange_interface.get_positions()
            new_positions = {}
            processed_symbols = set()
            for pos in current_positions_list:
                symbol = pos.get("symbol")
                if symbol in self.symbols and abs(float(pos.get("amount", 0.0))) > 1e-9:
                    if symbol not in processed_symbols:
                        new_positions[symbol] = pos
                        processed_symbols.add(symbol)
                    # else: logger.warning(...)

            # 更新 self.positions (单线程安全)
            log_updates = []
            # ... (比较新旧持仓，生成 log_updates)
            # 检查新增或变化的持仓
            for symbol, new_pos in new_positions.items():
                old_pos = self.positions.get(symbol)
                new_amount = float(new_pos.get("amount", 0))
                new_side = new_pos.get("side")
                old_amount = float(old_pos.get("amount", 0)) if old_pos else 0
                old_side = old_pos.get("side") if old_pos else None
                if abs(new_amount - old_amount) > 1e-9 or new_side != old_side:
                    log_updates.append(
                        f"{symbol}: {new_side or 'N/A'} {new_amount:.6f} (旧: {old_side or 'N/A'} {old_amount:.6f})"
                    )
            # 检查消失的持仓
            removed_symbols = set(self.positions.keys()) - set(new_positions.keys())
            for symbol in removed_symbols:
                old_pos = self.positions.get(symbol)
                old_amount = float(old_pos.get("amount", 0)) if old_pos else 0
                old_side = old_pos.get("side") if old_pos else None
                log_updates.append(
                    f"{symbol}: 平仓 (旧: {old_side or 'N/A'} {old_amount:.6f})"
                )

            self.positions = new_positions
            if log_updates:
                logger.info(f"持仓更新: {'; '.join(log_updates)}")
            # else: logger.debug("持仓无变化")

        except Exception as e:
            logger.error(f"更新持仓信息失败: {e}")
            logger.error(traceback.format_exc())

    async def execute_trades_based_on_signals(self):
        """(异步) 遍历信号并执行交易"""
        if self.signals is None or self.signals.empty:
            return
        processed_symbols = set()
        signals_to_process = self.signals.copy()

        for symbol, signal_info in signals_to_process.iterrows():
            if symbol in processed_symbols:
                continue
            # ... (检查 symbol 和 action) ...
            action = signal_info.get("action")
            if not action:
                continue

            latest_ticker = self.latest_tickers.get(symbol)
            if not latest_ticker or "price" not in latest_ticker:
                logger.warning(
                    f"[{symbol}] 无最新 Ticker 价格，无法执行信号 '{action}'"
                )
                continue
            # ... (获取 current_pos_info, current_side) ...
            current_pos_info = self.positions.get(symbol)
            current_side = current_pos_info.get("side") if current_pos_info else None
            current_amount = (
                float(current_pos_info.get("amount", 0.0)) if current_pos_info else 0.0
            )

            order_result = None
            order_status = "pending"
            error_msg = None
            signal_reason = signal_info.get("reason", "N/A")

            try:
                # --- 使用 await 调用异步下单方法 ---
                if action == "buy":
                    if current_side == "short":
                        logger.info(
                            f"[{symbol}] 信号: 开多 (原因: {signal_reason}). 平空仓..."
                        )
                        order_result = await self.exchange_interface.close_position(
                            symbol
                        )
                        if order_result and order_result.get("status") != "no_position":
                            order_status = "close_short_submitted"
                            await self.update_positions()
                            logger.info(f"[{symbol}] 尝试开多仓...")
                            order_result = (
                                await self.exchange_interface.place_buy_order(
                                    symbol, position_size=self.position_size
                                )
                            )
                            order_status = (
                                "buy_submitted"
                                if order_result
                                else "buy_error_after_close"
                            )
                        else:  # ... (处理平仓失败)
                            order_status = "close_short_failed"
                            error_msg = "..."
                    elif current_side != "long":
                        logger.info(
                            f"[{symbol}] 信号: 开多 (原因: {signal_reason}). 尝试开多仓..."
                        )
                        order_result = await self.exchange_interface.place_buy_order(
                            symbol, position_size=self.position_size
                        )
                        order_status = "buy_submitted" if order_result else "buy_error"
                    else:  # ... (处理已有多仓)
                        order_status = "ignored_already_long"
                elif action == "sell":
                    if current_side == "long":
                        logger.info(
                            f"[{symbol}] 信号: 开空 (原因: {signal_reason}). 平多仓..."
                        )
                        order_result = await self.exchange_interface.close_position(
                            symbol
                        )
                        if order_result and order_result.get("status") != "no_position":
                            order_status = "close_long_submitted"
                            await self.update_positions()
                            logger.info(f"[{symbol}] 尝试开空仓...")
                            order_result = (
                                await self.exchange_interface.place_sell_order(
                                    symbol, position_size=self.position_size
                                )
                            )
                            order_status = (
                                "sell_submitted"
                                if order_result
                                else "sell_error_after_close"
                            )
                        else:  # ... (处理平仓失败)
                            order_status = "close_long_failed"
                            error_msg = "..."
                    elif current_side != "short":
                        logger.info(
                            f"[{symbol}] 信号: 开空 (原因: {signal_reason}). 尝试开空仓..."
                        )
                        order_result = await self.exchange_interface.place_sell_order(
                            symbol, position_size=self.position_size
                        )
                        order_status = (
                            "sell_submitted" if order_result else "sell_error"
                        )
                    else:  # ... (处理已有空仓)
                        order_status = "ignored_already_short"
                elif action == "close":
                    if current_side is not None:
                        logger.info(
                            f"[{symbol}] 信号: 平仓 (原因: {signal_reason}). 尝试关闭 {current_side} 持仓..."
                        )
                        order_result = await self.exchange_interface.close_position(
                            symbol
                        )
                        if order_result:
                            order_status = (
                                "close_submitted"
                                if order_result.get("status") != "no_position"
                                else "close_no_position_needed"
                            )
                        else:
                            order_status = "close_error"
                    else:  # ... (处理无持仓)
                        order_status = "ignored_no_position"
                else:  # ... (处理未知 action)
                    order_status = "ignored_unknown_action"

            except Exception as e:
                logger.error(f"[{symbol}] 执行信号 '{action}' 时交易出错: {e}")
                logger.error(traceback.format_exc())
                order_status = "execution_exception"
                error_msg = str(e)

            # --- 记录订单 (同步) ---
            # ... (计算 req_amount, req_side) ...
            req_amount = (
                abs(current_amount) if action == "close" and current_pos_info else None
            )
            req_side = (
                action
                if action in ["buy", "sell"]
                else (
                    "sell"
                    if current_side == "long"
                    else "buy" if current_side == "short" else "none"
                )
            )
            # 注意：需要确保 order_result 可以被 json.dumps 序列化
            safe_order_result = (
                order_result
                if isinstance(order_result, (dict, type(None)))
                else str(order_result)
            )
            self.db.save_order(
                task_id=self.current_task_id,
                symbol=symbol,
                order_type=action,
                side=req_side,
                requested_amount=req_amount,
                position_size=self.position_size if action in ["buy", "sell"] else None,
                order_result=safe_order_result,
                status=order_status,
                error_message=error_msg,
            )

            if order_status in ["buy_submitted", "sell_submitted", "close_submitted"]:
                logger.info(f"[{symbol}] 订单已提交，立即更新持仓...")
                await self.update_positions()

            processed_symbols.add(symbol)

        # 清理已处理的信号 (同步)
        if (
            processed_symbols and self.signals is not None
        ):  # 检查 self.signals 是否仍存在
            rows_to_keep = self.signals.index.difference(processed_symbols)
            if not rows_to_keep.empty:
                self.signals = self.signals.loc[rows_to_keep]
            else:
                self.signals = None

    async def log_account_status(self):
        """(异步) 记录账户状态"""
        if not self.current_task_id:
            return
        try:
            # logger.debug("记录账户状态...")
            # 并发获取所需信息
            balance_info, positions_list = await asyncio.gather(
                self.exchange_interface.get_balance(),
                self.exchange_interface.get_positions(),
                return_exceptions=True,
            )

            if isinstance(balance_info, Exception):
                logger.error(f"记录账户状态时获取余额失败: {balance_info}")
                balance_info = {}  # 使用空字典继续
            if isinstance(positions_list, Exception):
                logger.error(f"记录账户状态时获取持仓失败: {positions_list}")
                positions_list = []  # 使用空列表继续

            # 同步处理数据和计算 PNL
            positions_dict = {
                p["symbol"]: p
                for p in positions_list
                if p.get("symbol") in self.symbols
            }
            pnl_info = {"total": {"unrealized_profit": 0.0}}
            total_pnl = 0.0
            for symbol, pos in positions_dict.items():
                pnl = float(pos.get("unrealized_pnl", 0.0))
                # 简化 pnl_info 的内容
                pnl_info[symbol] = {"unrealized_profit": pnl}
                total_pnl += pnl
            pnl_info["total"]["unrealized_profit"] = total_pnl

            # 保存数据库 (同步)
            self.db.save_account_status(
                task_id=self.current_task_id,
                balance_info=balance_info,
                pnl_info=pnl_info,
                positions_info=positions_dict,
            )
            # logger.debug("账户状态快照已记录")
        except Exception as e:
            logger.error(f"记录账户状态失败: {e}")
            logger.error(traceback.format_exc())
