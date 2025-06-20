import ccxt
import ccxt.pro as ccxtpro
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, AsyncGenerator, Union, Any
import asyncio
import time
import traceback
import sys
import random  # 添加 random 用于重连延迟
from decimal import Decimal, InvalidOperation as DecimalInvalidOperation  # 添加 Decimal
from utils.logger import logger

try:
    from ccxt.base.errors import NetworkError as CCXTNetworkError
    from ccxt.base.errors import RequestTimeout, ExchangeNotAvailable
except ImportError:
    # Fallback if import path is different or using older ccxt
    CCXTNetworkError = getattr(ccxt, "NetworkError", Exception)
    RequestTimeout = getattr(ccxt, "RequestTimeout", Exception)
    ExchangeNotAvailable = getattr(ccxt, "ExchangeNotAvailable", Exception)

# 移除 run_async_sync 辅助函数


class ExchangeInterface:
    # __init__ 需要修改，load_markets 需要在异步上下文中调用
    # 因此，创建一个异步的初始化辅助方法
    def __init__(
        self,
        exchange_name: str,
        api: str,
        secret: str,
        market_type: str = "future",
        testnet: bool = False,
        proxy_url: Optional[str] = None,  # <--- 必须有这一行
    ):
        self.exchange_name = exchange_name.lower()
        self.api_key = api
        self.secret_key = secret
        self.market_type = market_type.lower()
        self.testnet = testnet
        self.proxy_url = proxy_url  # <--- 必须有这一行，保存传入的 proxy_url
        self.exchange: Optional[ccxtpro.Exchange] = None
        self.min_position_value_usdt = 10.0
        # 不在同步的 __init__ 中加载 markets

    async def initialize(self):
        """异步初始化，创建交易所实例并加载市场"""
        try:
            self.exchange = self._create_exchange()  # _create_exchange 本身是同步的
            logger.info(
                f"交易所 {self.exchange_name} ({self.market_type}) 实例创建成功. 测试网: {self.testnet}"
            )
            await self._load_markets()  # 调用异步的 load_markets
            logger.info("市场信息异步加载完成.")
        except Exception as e:
            logger.error(f"异步初始化交易所接口 {self.exchange_name} 失败: {e}")
            logger.error(traceback.format_exc())
            raise

    def _create_exchange(self) -> ccxtpro.Exchange:
        """
        创建 ccxt.pro 交易所实例 (同时支持 REST 和 WebSocket)，并配置代理。

        :return: ccxt.pro 交易所实例
        """
        try:
            if not self.api_key or not self.secret_key:
                raise ValueError(
                    f"交易所 {self.exchange_name} 的 API Key 或 Secret Key 未提供"
                )

            exchange_class = getattr(ccxtpro, self.exchange_name)
            # 基础配置
            config = {
                "apiKey": self.api_key,
                "secret": self.secret_key,
                "enableRateLimit": True,  # 启用内置速率限制
                "options": {},
            }
            if self.market_type == "future":
                if self.exchange_name == "xt":
                    # logger.info("为 XT 合约市场设置 options (尝试 type='future', subType='linear')") # 日志级别降低或移除
                    config["options"]["defaultType"] = "future"
                    config["options"]["defaultSubType"] = "linear"
                else:
                    config["options"]["defaultType"] = "future"
            elif self.market_type == "spot":
                config["options"]["defaultType"] = "spot"
            else:
                raise ValueError(f"不支持的市场类型: {self.market_type}")

            # <<< --- 添加代理配置 --- >>>
            if self.proxy_url:
                logger.info(f"[{self.exchange_name}] 使用代理: {self.proxy_url}")
                # ccxt / ccxt.pro 通常期望 'proxies' 是一个字典
                proxies_dict = {
                    "http": self.proxy_url,
                    "https": self.proxy_url,
                    # 对于 socks 代理，ccxt 会自动处理 socks5:// 前缀
                }
                config["proxies"] = proxies_dict
                config["aiohttp_proxy"] = self.proxy_url
                # 对于基于 aiohttp 的 WebSocket (ccxt.pro 常用)，可能需要 'aiohttp_proxy'
                # 如果 'proxies' 对 WebSocket 无效，可以尝试这个
                # config['aiohttp_proxy'] = self.proxy_url
                logger.debug(
                    f"[{self.exchange_name}] ccxt 配置已包含代理设置: {config.get('proxies')}"
                )
            else:
                logger.info(f"[{self.exchange_name}] 不使用代理。")
            # <<< --- 代理配置结束 --- >>>

            # 实例化交易所
            logger.debug(f"[{self.exchange_name}] 准备使用配置实例化 ccxtpro: {config}")
            exchange = exchange_class(config)
            logger.info(
                f"交易所 {self.exchange_name} ({self.market_type}) 实例创建成功. 测试网: {self.testnet}, 代理: {self.proxy_url or '无'}"
            )

            # 设置测试网络模式
            if self.testnet:
                if hasattr(exchange, "set_sandbox_mode"):
                    try:
                        exchange.set_sandbox_mode(True)
                        logger.info(f"交易所 {self.exchange_name} 已设置为测试网络模式")
                    except Exception as sandbox_e:
                        logger.warning(
                            f"尝试为 {self.exchange_name} 设置测试网络模式时出错: {sandbox_e} (可能不支持)"
                        )
                else:
                    logger.warning(
                        f"交易所 {self.exchange_name} 不支持 set_sandbox_mode 方法，测试网络设置可能无效"
                    )

            return exchange

        except AttributeError:
            logger.error(f"不支持的交易所名称 (ccxt.pro): {self.exchange_name}")
            raise
        except ValueError as ve:
            logger.error(f"配置错误: {ve}")
            raise
        except Exception as e:
            logger.error(
                f"创建 ccxt.pro 交易所实例失败: {e}", exc_info=True
            )  # 添加 exc_info
            raise

    async def _load_markets(self, reload: bool = False):
        """加载市场信息，获取交易规则"""
        if not self.exchange:
            logger.warning(f"[{self.exchange_name}] 交易所实例未创建，无法加载市场。")
            return
        try:
            logger.info(f"[{self.exchange_name}] 尝试加载市场信息...")
            await self.exchange.load_markets(reload=reload)  # <--- 使用 await
            logger.info(f"交易所 {self.exchange_name} 的市场信息已加载")
        except (CCXTNetworkError, RequestTimeout) as net_err:
            logger.error(
                f"加载 {self.exchange_name} 市场信息时网络错误 (可能是代理问题): {net_err}"
            )
            # 根据需要，这里可以决定是否重试或直接抛出异常
            raise  # 重新抛出，让 initialize 处理
        except Exception as e:
            logger.error(f"加载 {self.exchange_name} 市场信息失败: {e}", exc_info=True)
            raise  # 重新抛出

    def _format_symbol(self, symbol: str) -> str:
        """
        根据交易所和市场类型格式化交易对符号。
        例如：将 'BTC/USDT' 转换为 'BTCUSDT' (如果需要)。
        """
        if not self.exchange:
            return symbol  # 如果交易所未初始化，返回原样

        # ccxt 通常会自动处理，但我们可以根据需要添加特定逻辑
        # if self.exchange_name in ['binance', 'okx', 'huobi'] and '/' in symbol:
        #     return symbol.replace('/', '')

        # 使用 ccxt 的 market 方法获取标准化的 symbol
        try:
            market = self.exchange.market(symbol)
            return market["symbol"]  # 返回交易所接受的标准格式
        except ccxt.BadSymbol:
            logger.warning(
                f"无法在 {self.exchange_name} 找到交易对 {symbol} 的市场信息，将使用原始符号"
            )
            return symbol
        except Exception as e:
            logger.error(f"格式化交易对 {symbol} 时出错: {e}")
            return symbol  # 出错时返回原始符号

    def _get_market_precision(self, symbol: str) -> Dict[str, Any]:
        """获取指定交易对的精度信息"""
        default_precision = {"amount": None, "price": None}
        if not self.exchange or not self.exchange.markets:
            return default_precision
        try:
            market = self.exchange.market(symbol)
            return market.get("precision", default_precision)
        except ccxt.BadSymbol:
            logger.warning(f"无法获取 {symbol} 的精度信息，将使用默认值")
            return default_precision
        except Exception as e:
            logger.error(f"获取 {symbol} 精度信息时出错: {e}")
            return default_precision

    def _get_market_limits(self, symbol: str) -> Dict[str, Any]:
        """获取指定交易对的限制信息 (最小/最大下单量/额)"""
        default_limits = {
            "amount": {"min": None, "max": None},
            "cost": {"min": None, "max": None},
        }
        if not self.exchange or not self.exchange.markets:
            return default_limits
        try:
            market = self.exchange.market(symbol)
            return market.get("limits", default_limits)
        except ccxt.BadSymbol:
            logger.warning(f"无法获取 {symbol} 的限制信息，将使用默认值")
            return default_limits
        except Exception as e:
            logger.error(f"获取 {symbol} 限制信息时出错: {e}")
            return default_limits

    # ========== 核心异步方法 (WebSocket) ==========

    async def watch_tickers(self, symbols: List[str]) -> AsyncGenerator[Dict, None]:
        """
        (再次修订)
        使用 WebSocket 监听 Ticker 更新。
        每次 await watch_tickers 返回一个包含更新的字典。
        """
        if not self.exchange or not hasattr(self.exchange, "watch_tickers"):
            logger.error(f"交易所 {self.exchange_name} 不支持 watch_tickers")
            return

        formatted_symbols = [self._format_symbol(s) for s in symbols]
        original_symbol_map = {self._format_symbol(s): s for s in symbols}
        logger.info(
            f"[{self.__class__.__name__}] 开始监听 Tickers: {formatted_symbols}"
        )

        connection_successful_once = False

        while True:  # 外层循环处理重连和获取
            try:
                # --- await 放在循环内部 ---
                # logger.debug(f"[{self.__class__.__name__}] 调用 await exchange.watch_tickers...")
                tickers_update = await self.exchange.watch_tickers(formatted_symbols)
                # logger.debug(f"[{self.__class__.__name__}] watch_tickers 返回类型: {type(tickers_update)}")

                if not connection_successful_once and tickers_update:
                    logger.info(
                        f"[{self.__class__.__name__}] WebSocket Ticker 流首次连接成功！"
                    )
                    connection_successful_once = True

                if isinstance(tickers_update, dict):
                    # --- 不再使用 async for，直接处理返回的字典 ---
                    for formatted_symbol, ticker_data in tickers_update.items():
                        original_symbol = original_symbol_map.get(
                            formatted_symbol, formatted_symbol
                        )
                        if not original_symbol:
                            continue

                        # --- 提取数据 ---
                        # ... (提取 ask_val, bid_val, last_val, timestamp_ms, dt, raw_info 的逻辑不变) ...
                        ask_val = None
                        bid_val = None
                        last_val = None
                        timestamp_ms = ticker_data.get("timestamp")
                        dt = ticker_data.get("datetime")
                        raw_info = ticker_data.get("info", {})
                        if isinstance(raw_info, dict):
                            bid_val = raw_info.get("bp") or raw_info.get("b")
                            ask_val = raw_info.get("ap") or raw_info.get("a")
                            last_val = raw_info.get("p")
                            if isinstance(raw_info.get("data"), dict):
                                bid_val = raw_info["data"].get("bp", bid_val)
                                ask_val = raw_info["data"].get("ap", ask_val)
                                last_val = raw_info["data"].get("p", last_val)
                            timestamp_ms = raw_info.get(
                                "t", timestamp_ms
                            ) or raw_info.get("E", timestamp_ms)
                            if dt is None and timestamp_ms:
                                try:
                                    dt = self.exchange.iso8601(timestamp_ms)
                                except:
                                    pass
                        if bid_val is None:
                            bid_val = ticker_data.get("bid")
                        if ask_val is None:
                            ask_val = ticker_data.get("ask")
                        if last_val is None:
                            last_val = ticker_data.get("last")

                        yield_data = {
                            "symbol": original_symbol,
                            "timestamp": timestamp_ms,
                            "datetime": dt,
                            "last": None,
                            "bid": None,
                            "ask": None,
                            "info": raw_info,
                        }
                        # --- 转换并验证价格 ---
                        try:
                            if last_val is not None:
                                yield_data["last"] = float(last_val)
                            if bid_val is not None:
                                yield_data["bid"] = float(bid_val)
                            if ask_val is not None:
                                yield_data["ask"] = float(ask_val)

                            # --- 验证并 yield ---
                            if yield_data["last"] is None and (
                                yield_data["bid"] is None or yield_data["ask"] is None
                            ):
                                pass
                            elif (
                                (
                                    yield_data["bid"] is not None
                                    and yield_data["bid"] <= 0
                                )
                                or (
                                    yield_data["ask"] is not None
                                    and yield_data["ask"] <= 0
                                )
                                or (
                                    yield_data["bid"] is not None
                                    and yield_data["ask"] is not None
                                    and yield_data["ask"] < yield_data["bid"]
                                )
                            ):
                                logger.warning(
                                    f"[{self.__class__.__name__}|{original_symbol}] Ticker 价格无效: bid={yield_data['bid']}, ask={yield_data['ask']}"
                                )
                            else:
                                yield yield_data  # 产生数据

                        except (ValueError, TypeError) as conv_e:
                            logger.warning(
                                f"转换 {original_symbol} ticker价格时出错: {conv_e}"
                            )
                # else: logger.warning(f"watch_tickers 返回了非字典类型: {type(tickers_update)}")

                # --- !! 重要：在内层循环加一个短暂 sleep，避免CPU空转 !! ---
                # 因为 watch_tickers 现在每次返回 dict，我们需要自己控制轮询频率
                # 这个值需要调整，太小可能导致频繁请求，太大则延迟高
                await asyncio.sleep(0.05)  # 尝试 50ms

            # --- 异常处理 (保持不变) ---
            except (
                CCXTNetworkError,
                RequestTimeout,
                ConnectionRefusedError,
                ConnectionResetError,
                asyncio.TimeoutError,
            ) as e:
                logger.warning(
                    f"[{self.__class__.__name__}] WebSocket 网络/超时错误: {type(e).__name__} - {e}. 尝试重新连接..."
                )
                connection_successful_once = False
                await asyncio.sleep(5 + random.uniform(0, 2))
                continue
            except ExchangeNotAvailable as e:
                logger.error(
                    f"[{self.__class__.__name__}] 交易所暂时不可用: {e}. 等待后重试..."
                )
                connection_successful_once = False
                await asyncio.sleep(30 + random.uniform(0, 5))
                continue
            except asyncio.CancelledError:
                logger.info(f"[{self.__class__.__name__}] watch_tickers 任务被取消。")
                break
            except Exception as e:
                logger.error(
                    f"[{self.__class__.__name__}] watch_tickers 发生未知错误: {e}",
                    exc_info=True,
                )
                connection_successful_once = False
                await asyncio.sleep(15 + random.uniform(0, 5))
                continue

        logger.warning(f"[{self.__class__.__name__}] watch_tickers 监听循环已停止。")

    async def close(self):
        """
        异步关闭 ccxt.pro 的 WebSocket 连接。
        """
        if (
            self.exchange
            and hasattr(self.exchange, "close")
            and asyncio.iscoroutinefunction(self.exchange.close)
        ):
            try:
                logger.info(f"正在关闭交易所 {self.exchange_name} 的 WebSocket 连接...")
                await self.exchange.close()
                logger.info(f"交易所 {self.exchange_name} WebSocket 连接已关闭")
            except Exception as e:
                logger.error(f"关闭交易所 {self.exchange_name} 连接时出错: {e}")
        else:
            logger.warning(
                f"交易所 {self.exchange_name} 实例无法关闭或没有异步 close 方法"
            )

    # ========== 核心同步方法 (REST API) ==========

    async def fetch_ohlcv(
        self, symbol: str, timeframe: str = "1m", limit: int = 1000
    ) -> pd.DataFrame:
        """
        同步获取 K 线数据 (OHLCV)。

        :param symbol: 交易对 (例如 'BTC/USDT')
        :param timeframe: K 线周期 (例如 '1m', '5m', '1h')
        :param limit: 获取的 K 线数量
        :return: K 线数据的 Pandas DataFrame (索引为 DatetimeIndex UTC)，失败时返回空 DataFrame
        """
        if not self.exchange:
            return pd.DataFrame()

        formatted_symbol = self._format_symbol(symbol)  # 获取交易所接受的格式
        market_id = None
        try:  # 尝试获取 market id
            market_id = self.exchange.market(symbol)["id"]
        except:
            pass
        logger.debug(
            f"尝试获取 K 线: input='{symbol}', formatted='{formatted_symbol}', market_id='{market_id}', timeframe='{timeframe}', limit={limit}"
        )
        try:
            ohlcv = await self.exchange.fetch_ohlcv(
                formatted_symbol, timeframe, limit=limit
            )

            # --- 检查返回结果 ---
            if ohlcv:
                logger.debug(
                    f"API fetch_ohlcv for '{formatted_symbol}' 返回了 {len(ohlcv)} 条原始数据."
                )
                # --- DataFrame 转换 ---
                try:
                    df = pd.DataFrame(
                        ohlcv,
                        columns=["timestamp", "open", "high", "low", "close", "volume"],
                    )
                    if df.empty:
                        logger.warning(
                            f"原始 K 线数据转换后 DataFrame 为空 for '{formatted_symbol}'"
                        )
                        return pd.DataFrame()
                    df["timestamp"] = pd.to_datetime(
                        df["timestamp"], unit="ms", utc=True
                    )
                    df.set_index("timestamp", inplace=True)
                    for col in ["open", "high", "low", "close", "volume"]:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    # 再次检查转换后是否为空
                    if df.empty:
                        logger.warning(
                            f"K 线数据 to_numeric/set_index 后 DataFrame 为空 for '{formatted_symbol}'"
                        )
                    # logger.debug(f"成功将 K 线数据转换为 DataFrame for '{formatted_symbol}', shape={df.shape}")
                    return df
                except Exception as df_e:
                    logger.error(
                        f"将 K 线数据转换为 DataFrame 时出错 for '{formatted_symbol}': {df_e}"
                    )
                    logger.error(
                        f"原始 ohlcv 数据 (前5条): {ohlcv[:5] if ohlcv else 'N/A'}"
                    )
                    logger.error(traceback.format_exc())
                    return pd.DataFrame()
            else:
                # API 直接返回了空列表或 None
                logger.warning(
                    f"API fetch_ohlcv for '{formatted_symbol}' 返回了空数据 (None or empty list)"
                )
                return pd.DataFrame()
        except Exception as e:
            logger.error(
                f"调用 fetch_ohlcv 时发生异常 ({formatted_symbol}, {timeframe}): {e}"
            )
            logger.error(traceback.format_exc())
            return pd.DataFrame()

    async def get_balance(self) -> Dict:
        """
        同步获取账户余额信息。

        :return: 包含余额信息的字典，格式:
                 {
                     'total': { 'USDT': 1000, 'BTC': 0.1, ... },
                     'free': { 'USDT': 500, 'BTC': 0.1, ... },
                     'used': { 'USDT': 500, 'BTC': 0, ... },
                     'total_value_usdt': 1500.0, # 估算的账户总价值 (USDT)
                     'info': { ... } # 原始交易所返回信息
                 }
                 失败时返回空字典。
        """
        if not self.exchange:
            return {}
        try:
            params = {}
            # --- 直接 await 调用 ---
            balance = await self.exchange.fetch_balance(params=params)

            result = {
                "total": balance.get("total", {}),
                "free": balance.get("free", {}),
                "used": balance.get("used", {}),
                "info": balance.get("info", {}),
            }

            # --- 计算账户总价值 (以 USDT 估算) ---
            total_value_usdt = 0.0
            quote_currency = "USDT"  # 主要计价货币

            if self.market_type == "future":
                # 优先尝试从 'info' 中获取交易所提供的总保证金余额或权益
                info = result["info"]
                if self.exchange_name == "binance" and "totalWalletBalance" in info:
                    # Binance U本位合约的总钱包余额 (保证金)
                    total_value_usdt = float(info["totalWalletBalance"])
                elif self.exchange_name == "binance" and "totalMarginBalance" in info:
                    # 可能的总保证金余额字段
                    total_value_usdt = float(info["totalMarginBalance"])
                elif self.exchange_name == "okx" and "totalEq" in info:
                    # OKX 的总权益 (通常在 info.data[0] 中)
                    if isinstance(info.get("data"), list) and len(info["data"]) > 0:
                        total_value_usdt = float(info["data"][0].get("totalEq", 0.0))
                # ... 可以为其他交易所添加特定字段 ...
                elif quote_currency in result["total"]:
                    # 如果找不到特定字段，退而求其次使用计价货币的总量作为估算
                    # 注意：这可能不完全准确，因为它不包括未实现盈亏
                    total_value_usdt = result["total"][quote_currency]
                    logger.warning(
                        f"无法直接获取 {self.exchange_name} 期货账户总价值，使用 {quote_currency} 余额作为估算: {total_value_usdt}"
                    )
                else:
                    # logger.warning(f"无法估算 {self.exchange_name} 期货账户总价值")
                    # 可以选择进一步通过持仓计算，但会更复杂
                    total_value_usdt = self._calculate_total_value_from_tickers(
                        result["total"], quote_currency
                    )

            else:  # 现货账户
                total_value_usdt = self._calculate_total_value_from_tickers(
                    result["total"], quote_currency
                )

            result["total_value_usdt"] = total_value_usdt
            # logger.info(f"账户总资产估值: {total_value_usdt:.2f} {quote_currency}")
            return result

        except Exception as e:
            logger.error(f"获取账户余额信息失败: {e}")
            logger.error(traceback.format_exc())
            return {}

    async def _calculate_total_value_from_tickers(
        self, total_balances: Dict, quote_currency: str
    ) -> float:
        """(辅助方法) 根据各币种余额和市场最新价格计算总价值 (主要用于现货或作为期货备用)"""
        if not self.exchange:
            return 0.0
        total_value = 0.0
        if quote_currency in total_balances:
            # 确保金额是 float 类型
            try:
                total_value += float(total_balances[quote_currency])
            except (ValueError, TypeError):
                logger.warning(
                    f"无法将 {quote_currency} 余额 '{total_balances[quote_currency]}' 转换为 float"
                )

        for currency, amount_raw in total_balances.items():
            try:
                amount = float(amount_raw)  # 确保 amount 是 float
            except (ValueError, TypeError):
                logger.warning(
                    f"无法将 {currency} 余额 '{amount_raw}' 转换为 float，跳过计算其价值"
                )
                continue

            if currency == quote_currency or amount <= 1e-9:  # 增加一个小的阈值判断
                continue
            try:
                symbol = f"{currency}/{quote_currency}"
                formatted_symbol = self._format_symbol(symbol)  # 获取交易所标准格式
                # logger.debug(f"Fetching ticker for {formatted_symbol} to calculate value...")
                # 调用异步 fetch_ticker
                ticker = await self.exchange.fetch_ticker(formatted_symbol)
                # logger.debug(f"Ticker fetched for {formatted_symbol}: {ticker}")

                price = (
                    ticker.get("last") or ticker.get("close") or ticker.get("bid")
                )  # 尝试 bid 作为备用
                if price is not None and float(price) > 0:
                    value = amount * float(price)
                    # logger.debug(f"Value for {currency}: {amount} * {price} = {value}")
                    total_value += value
                else:
                    logger.warning(
                        f"无法获取 {formatted_symbol} 的有效价格信息 (last/close/bid is None or <= 0)，跳过计算其价值. Ticker: {ticker}"
                    )
            except ccxt.BadSymbol:
                # logger.debug(f"不支持的交易对 {symbol}，无法计算其价值")
                pass  # 忽略无法交易的币种
            except (CCXTNetworkError, RequestTimeout) as net_err:
                logger.warning(
                    f"计算 {currency} 的 {quote_currency} 价值时网络错误 (可能是代理): {net_err}"
                )
            except Exception as e:
                logger.warning(
                    f"计算 {currency} 的 {quote_currency} 价值时出错: {e}",
                    exc_info=True,
                )
        # logger.debug(f"Calculated total value from tickers: {total_value}")
        return total_value

    async def get_positions(self, symbol: Optional[str] = None) -> List[Dict]:
        if not self.exchange:
            return []
        if self.market_type != "future":
            return []
        try:
            params = {}
            target_symbol = self._format_symbol(symbol) if symbol else None
            # --- 直接 await 调用 ---
            positions_raw = await self.exchange.fetch_positions(
                symbols=[target_symbol] if target_symbol else None, params=params
            )

            # 过滤掉数量为 0 或价值过小的持仓
            filtered_positions = []
            if positions_raw:  # 确保返回不是 None 或其他非可迭代对象
                for pos in positions:
                    # ccxt 返回的 amount 可能包含多空符号，contracts 是绝对值
                    amount = float(
                        pos.get("contracts", 0.0)
                    )  # 使用 contracts 作为绝对数量
                    side = pos.get("side")  # 'long' or 'short'
                    if amount > 0 and side is not None:  # 确保有实际持仓和方向
                        entry_price = float(pos.get("entryPrice", 0.0))
                        # 计算持仓价值 (使用标记价格更准确)
                        mark_price = float(pos.get("markPrice", entry_price))
                        contract_size = float(pos.get("contractSize", 1.0))  # 合约面值
                        position_value = amount * mark_price * contract_size

                        # 过滤价值过小的持仓
                        if position_value >= self.min_position_value_usdt:
                            # 构造标准化的持仓信息字典
                            position_info = {
                                "symbol": pos.get("symbol"),
                                "side": side,
                                "amount": amount
                                * (1 if side == "long" else -1),  # 带符号的数量
                                "contracts": amount,  # 合约数量（绝对值）
                                "entry_price": entry_price,
                                "mark_price": mark_price,
                                "liquidation_price": pos.get("liquidationPrice"),
                                "leverage": pos.get("leverage"),
                                "unrealized_pnl": pos.get("unrealizedPnl"),
                                "position_value": position_value,
                                "info": pos.get("info", {}),  # 保留原始信息
                            }
                            filtered_positions.append(position_info)

            # logger.debug(f"获取到 {len(filtered_positions)} 个有效持仓")
            return filtered_positions

        except ccxt.NotSupported as e:
            logger.error(f"交易所 {self.exchange_name} 不支持 fetch_positions: {e}")
            return []
        except Exception as e:
            logger.error(f"获取持仓信息失败 ({symbol or '所有'}): {e}")
            return []

    async def get_pnl(self) -> Dict:
        """
        异步计算账户的总未实现盈亏 (基于当前持仓)。
        (修改为异步，因为它依赖异步的 get_positions)
        """
        pnl_info = {"total": {"unrealized_profit": 0.0}}
        try:
            positions = await self.get_positions()  # 调用异步版本
            total_unrealized_pnl = 0.0

            for pos in positions:
                symbol = pos.get("symbol")
                unrealized_profit = float(pos.get("unrealized_pnl", 0.0))
                if symbol:
                    pnl_info[symbol] = {
                        "unrealized_profit": unrealized_profit,
                        "side": pos.get("side"),
                        # 使用 contracts 更合适代表数量绝对值
                        "contracts": pos.get("contracts"),
                        "position_value": pos.get("position_value"),
                    }
                    total_unrealized_pnl += unrealized_profit

            pnl_info["total"]["unrealized_profit"] = total_unrealized_pnl
            # logger.info(f"当前总未实现盈亏: {total_unrealized_pnl:.4f}")
            return pnl_info

        except Exception as e:
            logger.error(f"计算账户盈亏失败: {e}")
            return {}

    async def set_leverage(self, symbol: str, leverage: int) -> Optional[Dict]:
        if not self.exchange:
            return None
        if self.market_type != "future":
            logger.error("设置杠杆功能仅适用于期货市场")
            return None

        formatted_symbol = self._format_symbol(symbol)
        results = {}  # 存储多空设置结果

        async def _set_side_leverage(side):
            params = {}
            if self.exchange_name == "xt":  # XT 特定参数
                params["positionSide"] = side.upper()
            try:
                # 使用 await 调用异步方法
                response = await self.exchange.set_leverage(
                    leverage, formatted_symbol, params=params
                )
                logger.info(
                    f"尝试为 {formatted_symbol} ({side}) 设置杠杆为 {leverage}x 成功: {response}"
                )
                return response
            except ccxt.NotSupported:
                logger.warning(
                    f"交易所 {self.exchange_name} ({side}) 不支持 set_leverage 或需要特定参数。"
                )
                return None
            except ccxt.ExchangeError as ee:
                # 如果错误信息包含 "positionSide" 或类似内容，可能是 XT 的要求
                if "positionSide" in str(ee) and self.exchange_name != "xt":
                    logger.warning(
                        f"设置杠杆时交易所 {self.exchange_name} 可能需要 positionSide 参数: {ee}"
                    )
                else:
                    logger.error(
                        f"设置杠杆时交易所返回错误 ({formatted_symbol}, {leverage}x, {side}): {ee}"
                    )
                return None
            except Exception as e:
                logger.error(
                    f"设置杠杆失败 ({formatted_symbol}, {leverage}x, {side}): {e}"
                )
                return None

        # 尝试为多头和空头都设置杠杆 (XT 需要)
        results["long"] = await _set_side_leverage("long")
        await asyncio.sleep(0.1)  # 短暂间隔，避免限频
        results["short"] = await _set_side_leverage("short")

        # 返回一个综合结果，或者根据需要决定如何处理部分成功
        if results["long"] or results["short"]:
            return {"long_result": results["long"], "short_result": results["short"]}
        else:
            return None  # 如果双向都失败

    # ========== 下单方法 ==========

    async def _create_order(
        self,
        symbol: str,
        type: str,
        side: str,
        amount: float,
        price: Optional[float] = None,
        params: Dict = {},
    ) -> Optional[Dict]:
        if not self.exchange:
            return None
        formatted_symbol = self._format_symbol(symbol)
        try:
            order = None
            logger.info(
                f"[async] 准备创建订单: {formatted_symbol} {type} {side} {amount} price={price}"
            )
            if type.lower() == "limit":
                if price is None:
                    raise ValueError("限价单需要提供价格")
                # --- 直接 await ---
                order = await self.exchange.create_limit_order(
                    formatted_symbol, side, amount, price, params
                )
            elif type.lower() == "market":
                # --- 直接 await ---
                order = await self.exchange.create_market_order(
                    formatted_symbol, side, amount, params
                )
            else:
                raise ValueError(f"不支持的订单类型: {type}")

            if order:
                logger.info(f"[async] 创建订单成功: {order.get('id')}")
            else:
                logger.warning(f"[async] 创建订单调用返回 None")
            return ordere

        except ccxt.InsufficientFunds as e:
            logger.error(
                f"下单失败：资金不足 ({formatted_symbol}, {type}, {side}, {amount}): {e}"
            )
            return None  # 返回 None 表示失败
        except ccxt.InvalidOrder as e:
            logger.error(
                f"下单失败：无效订单 ({formatted_symbol}, {type}, {side}, {amount}, price={price}): {e}"
            )
            return None
        except ccxt.ExchangeError as e:
            logger.error(
                f"下单时交易所错误 ({formatted_symbol}, {type}, {side}, {amount}): {e}"
            )
            return None
        except Exception as e:
            logger.error(
                f"下单时发生未知错误 ({formatted_symbol}, {type}, {side}, {amount}): {e}"
            )
            return None

    async def _calculate_order_amount(
        self,
        symbol: str,
        side: str,  # 'buy' for open long, 'sell' for open short
        position_size: Optional[float] = None,
        close_position: bool = False,
        current_leverage: Optional[int] = None,  # 显式传入当前杠杆
    ) -> Optional[float]:
        """(异步) 计算下单数量"""
        if not self.exchange:
            return None
        formatted_symbol = self._format_symbol(symbol)

        if close_position:
            # 平仓：获取当前持仓数量
            positions = await self.get_positions(symbol=symbol)  # 调用异步版本
            if not positions:
                logger.warning(f"[{formatted_symbol}] 尝试平仓，但未找到持仓信息")
                return None  # 返回 None 表示无需平仓或无法获取持仓

            current_pos = positions[0]
            # 使用 'contracts' (绝对值)
            amount_to_close = float(current_pos.get("contracts", 0.0))
            if amount_to_close <= 1e-9:
                logger.warning(
                    f"[{formatted_symbol}] 找到持仓但数量为 0，无需平仓。Pos: {current_pos}"
                )
                return None
            logger.info(
                f"[{formatted_symbol}] 计算平仓数量 (合约数): {amount_to_close}"
            )
            return amount_to_close
        else:
            # 开仓：根据 position_size 计算
            if position_size is None or not (0 < position_size <= 1):
                logger.error(
                    f"[{formatted_symbol}] 开仓需要有效的 position_size (0, 1]，当前值: {position_size}"
                )
                return None

            try:
                balance = await self.get_balance()  # 调用异步版本
                # 使用可用保证金 (USDT) - 注意: 合约账户的 free 可能不直接是 USDT
                # 优先使用 total_value_usdt 估算总权益，然后计算可用保证金
                # 这是一个简化，更精确的计算需要考虑维持保证金等
                total_equity_usdt = balance.get("total_value_usdt", 0.0)
                used_margin_usdt = balance.get("used", {}).get(
                    "USDT", 0.0
                )  # 近似已用保证金
                # 近似可用保证金
                free_margin_approx = max(0.0, total_equity_usdt - used_margin_usdt)

                # 或者直接用 free USDT (如果交易所提供了有意义的值)
                free_usdt = balance.get("free", {}).get("USDT", 0.0)
                # 选择一个更可靠的可用资金指标
                margin_to_use = free_usdt  # 假设 free USDT 是可用的保证金

                if margin_to_use <= 1e-6:  # 增加阈值判断
                    logger.error(
                        f"[{formatted_symbol}] 可用保证金不足 (估算 USDT: {margin_to_use:.4f})，无法计算开仓数量. Balance: {balance}"
                    )
                    return None

                # 获取最新价格
                ticker = await self.exchange.fetch_ticker(
                    formatted_symbol
                )  # 调用异步版本
                # 使用买一价开空，卖一价开多，如果不存在则用 last
                price_for_calc = None
                if side == "buy":  # 开多用卖一价计算成本
                    price_for_calc = (
                        ticker.get("ask") or ticker.get("last") or ticker.get("close")
                    )
                elif side == "sell":  # 开空用买一价计算成本
                    price_for_calc = (
                        ticker.get("bid") or ticker.get("last") or ticker.get("close")
                    )

                if not price_for_calc or float(price_for_calc) <= 0:
                    logger.error(
                        f"[{formatted_symbol}] 无法获取有效的最新价格 (ask/bid/last: {ticker.get('ask')}/{ticker.get('bid')}/{ticker.get('last')})，无法计算开仓数量"
                    )
                    return None
                current_price = float(price_for_calc)

                # 获取杠杆
                leverage = current_leverage or await self.get_current_leverage(
                    symbol
                )  # 调用异步版本

                # 计算可开仓的名义价值
                nominal_value = margin_to_use * position_size * leverage
                # 计算可开仓的基础货币数量 (合约数量)
                # 注意: 需要合约面值 (contractSize) 来精确计算
                market = self.exchange.market(formatted_symbol)
                contract_size = float(market.get("contractSize", 1.0))

                # amount 是合约数量
                amount = nominal_value / (current_price * contract_size)

                # 应用最小下单量限制
                limits = self._get_market_limits(formatted_symbol)
                min_amount_contracts = limits.get("amount", {}).get(
                    "min"
                )  # 这是合约数量的最小限制
                if min_amount_contracts and amount < min_amount_contracts:
                    logger.warning(
                        f"[{formatted_symbol}] 计算得到的开仓合约数量 {amount:.8f} 小于最小允许值 {min_amount_contracts}，调整为最小值"
                    )
                    amount = min_amount_contracts

                # 应用数量精度 (合约数量精度)
                amount = float(
                    self.exchange.amount_to_precision(formatted_symbol, amount)
                )

                # 再次检查调整后是否大于0
                if amount <= 1e-9:
                    logger.warning(
                        f"[{formatted_symbol}] 调整精度/最小值后计算得到的数量过小 ({amount:.8f})，无法开仓。"
                    )
                    return None

                logger.info(
                    f"[{formatted_symbol}] 计算开仓数量: 可用保证金≈{margin_to_use:.2f}, 仓位={position_size*100}%, "
                    f"杠杆={leverage}x, 计算价格={current_price:.4f}, 合约面值={contract_size}, "
                    f"计算合约数={amount:.8f}"
                )
                return amount

            except (CCXTNetworkError, RequestTimeout) as net_err:
                logger.error(
                    f"[{formatted_symbol}] 计算开仓数量时网络错误 (可能是代理): {net_err}"
                )
                return None
            except Exception as e:
                logger.error(
                    f"[{formatted_symbol}] 计算开仓数量时出错: {e}", exc_info=True
                )
                return None

    # --- 需要实现或完善 ---
    async def get_current_leverage(self, symbol: str) -> int:
        logger.warning("[async] get_current_leverage 未完全实现，返回默认杠杆 10")
        await asyncio.sleep(0)  # 模拟异步操作
        return 10

    def fetch_ticker(self, symbol: str) -> Optional[Dict]:
        if not self.exchange:
            return None
        formatted_symbol = self._format_symbol(symbol)
        try:
            # --- 同步执行异步的 fetch_ticker ---
            ticker = run_async_sync(self.exchange.fetch_ticker(formatted_symbol))
            return ticker
        except Exception as e:
            logger.error(f"获取 ticker 失败 ({formatted_symbol}): {e}")
            return None

    async def place_buy_order(
        self,
        symbol: str,
        amount: Optional[float] = None,  # 优先使用 amount
        price: Optional[float] = None,  # 限价单价格
        order_type: str = "market",  # 'market' or 'limit'
        position_size: Optional[float] = None,  # 如果 amount 未提供，则使用此比例计算
    ) -> Optional[Dict]:
        """
        同步下买单 (开多仓)。

        :param symbol: 交易对
        :param amount: 购买数量 (基础货币)，如果为 None 则根据 position_size 计算
        :param price: 限价单价格
        :param order_type: 'market' 或 'limit'
        :param position_size: 仓位大小 (0, 1]，例如 0.1 表示使用 10% 可用保证金开仓
        :return: 交易所返回的订单信息字典，如果失败则返回 None
        """
        # --- 暂时保持同步调用（非最佳） ---
        # amount = self._calculate_order_amount(symbol, side='buy', position_size=position_size)
        # --- 正确做法：await 异步版本 ---
        # amount = await self._calculate_order_amount(symbol, side='buy', position_size=position_size)
        # --- 暂时保持同步调用 ---
        amount_sync = self._calculate_order_amount(
            symbol, side="buy", position_size=position_size
        )
        if amount_sync is None:
            return None
        if amount_sync <= 0:
            return None

        params = {"reduceOnly": False}
        # --- 调用异步的 _create_order ---
        return await self._create_order(
            symbol, order_type, "buy", amount_sync, price, params
        )

    async def place_sell_order(
        self,
        symbol: str,
        amount: Optional[float] = None,  # 优先使用 amount
        price: Optional[float] = None,  # 限价单价格
        order_type: str = "market",  # 'market' or 'limit'
        position_size: Optional[float] = None,  # 如果 amount 未提供，则使用此比例计算
    ) -> Optional[Dict]:
        """
        同步下卖单 (开空仓)。

        :param symbol: 交易对
        :param amount: 卖出数量 (基础货币)，如果为 None 则根据 position_size 计算
        :param price: 限价单价格
        :param order_type: 'market' 或 'limit'
        :param position_size: 仓位大小 (0, 1]，例如 0.1 表示使用 10% 可用保证金开仓
        :return: 交易所返回的订单信息字典，如果失败则返回 None
        """
        amount_sync = self._calculate_order_amount(
            symbol, side="sell", position_size=position_size
        )
        if amount_sync is None:
            return None
        if amount_sync <= 0:
            return None

        params = {"reduceOnly": False}
        # --- 调用异步的 _create_order ---
        return await self._create_order(
            symbol, order_type, "sell", amount_sync, price, params
        )

    async def close_position(
        self, symbol: str, price: Optional[float] = None, order_type: str = "market"
    ) -> Optional[Dict]:
        """
        同步平掉指定交易对的所有持仓。

        :param symbol: 交易对
        :param price: 平仓限价单的价格 (如果 order_type='limit')
        :param order_type: 'market' 或 'limit'
        :return: 交易所返回的订单信息字典，如果无需平仓或失败则返回 None 或特定状态字典
        """
        positions = run_async_sync(self.get_positions(symbol=symbol))
        if not positions:
            return {"status": "no_position"}

        current_pos = positions[0]
        amount_to_close = abs(float(positions[0].get("amount", 0.0)))
        current_side = positions[0].get("side")
        if amount_to_close <= 0 or not current_side:
            return None
        close_side = "sell" if current_side == "long" else "buy"

        params = {"reduceOnly": True}
        # --- 调用异步的 _create_order ---
        order_result = await self._create_order(
            symbol, order_type, close_side, amount_to_close, price, params
        )

        if order_result is None and order_type == "market":
            # 如果市价平仓失败，可以尝试限价平仓作为后备 (需要更复杂的逻辑)
            logger.error(f"[{symbol}] 市价平仓失败，请检查原因。")

        return order_result

    async def cancel_order(self, order_id: str, symbol: str) -> Optional[Dict]:
        """
        同步取消订单。

        :param order_id: 要取消的订单 ID
        :param symbol: 订单所属的交易对
        :return: 取消结果字典，失败返回 None
        """
        if not self.exchange:
            return None
        formatted_symbol = self._format_symbol(symbol)
        try:
            result = await self.exchange.cancel_order(order_id, formatted_symbol)
            logger.info(f"取消订单 {order_id} ({formatted_symbol}) 成功: {result}")
            return result
        except ccxt.OrderNotFound:
            logger.warning(
                f"取消订单失败：订单 {order_id} ({formatted_symbol}) 未找到或已完成/取消"
            )
            return None
        except Exception as e:
            logger.error(f"取消订单 {order_id} ({formatted_symbol}) 失败: {e}")
            return None

    # --- 其他辅助方法 ---
    async def fetch_my_trades(
        self,
        symbol: Optional[str] = None,
        since: Optional[int] = None,  # 开始时间戳 (毫秒)
        limit: Optional[int] = 100,
        params: Dict = {},
    ) -> List[Dict]:
        """
        同步获取账户的成交记录 (Fills)。

        :param symbol: 可选，过滤特定交易对
        :param since: 可选，获取此时间戳之后的成交记录
        :param limit: 返回的最大记录数
        :param params: 传递给 ccxt fetchMyTrades 的额外参数
        :return: 成交记录字典列表，失败返回空列表
        """
        if not self.exchange:
            return []
        formatted_symbol = self._format_symbol(symbol) if symbol else None
        try:
            # 添加 market_type 参数（如果交易所支持）
            if self.market_type == "future":
                params["type"] = "future"  # 或 'swap'
            elif self.market_type == "spot":
                params["type"] = "spot"

            trades = await self.exchange.fetch_my_trades(
                formatted_symbol, since, limit, params
            )
            logger.info(f"获取到 {len(trades)} 条成交记录 ({symbol or '所有'})")
            # 可以选择在这里格式化 trades 字典
            return trades
        except Exception as e:
            logger.error(f"获取成交记录失败 ({symbol or '所有'}): {e}")
            return []

    async def fetch_order(self, order_id: str, symbol: str) -> Optional[Dict]:
        """
        同步获取单个订单的详细信息。

        :param order_id: 订单 ID
        :param symbol: 交易对
        :return: 订单信息字典，失败返回 None
        """
        if not self.exchange:
            return None
        formatted_symbol = self._format_symbol(symbol)
        try:
            order = await self.exchange.fetch_order(order_id, formatted_symbol)
            return order
        except ccxt.OrderNotFound:
            logger.warning(
                f"获取订单详情失败：订单 {order_id} ({formatted_symbol}) 未找到"
            )
            return None
        except Exception as e:
            logger.error(f"获取订单 {order_id} ({formatted_symbol}) 详情失败: {e}")
            return None
