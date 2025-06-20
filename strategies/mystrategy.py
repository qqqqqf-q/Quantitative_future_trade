# 文件: strategies/mystrategy.py

import pandas as pd
from datetime import timedelta, datetime
from typing import Dict, Optional


class MyStrategy:
    """做市套利策略"""

    def __init__(self):
        # 策略名称
        self.name = "MarketMakingArb"
        # 挂单偏移 δ（0.03%）
        self.delta = 0.0003
        # 触发挂单的最小价差阈值（0.05%）
        self.entry_spread = 0.0005
        # 平仓目标价差阈值（0.03%）
        self.exit_spread = 0.0003
        # 最大持仓 USDT
        self.max_notional = 100000
        # 最大挂单持仓时间
        self.max_holding = timedelta(seconds=60)
        # 强制平仓时长阈值（30 分钟）
        self.time_exit = timedelta(minutes=30)
        # 止损反向滑点阈值（0.05%）
        self.stop_loss = 0.0005
        # 资金费套利缓冲时间（3 分钟）
        self.funding_buffer = timedelta(minutes=3)
        # 每个交易对的状态记录
        self.state: Dict[str, Dict] = {}

    def _init_state(self, symbol: str):
        if symbol not in self.state:
            self.state[symbol] = {
                "position": 0,  # 1: 多, -1: 空, 0: 无仓
                "entry_price": None,  # 入场价格
                "entry_time": None,  # 入场时间戳
                "pending": [],  # 挂单列表: [(类型, 价格)]
                "size": 0.0,  # 仓位规模 (USDT)
            }

    def _mid_price(self, ob: pd.DataFrame) -> float:
        # 计算盘口中间价
        return (ob["bid"].iloc[0] + ob["ask"].iloc[0]) / 2

    def _spread(self, ob: pd.DataFrame) -> float:
        # 计算当前买卖价差百分比
        return (ob["ask"].iloc[0] - ob["bid"].iloc[0]) / ob["bid"].iloc[0]

    def _in_funding_window(self, symbol: str, now: datetime) -> bool:
        # TODO: 根据交易所资金费时间表判断，示例中始终返回 False
        return False

    def generate_signals(
        self, market_data: Dict[str, pd.DataFrame]
    ) -> Optional[pd.DataFrame]:
        """
        market_data: 每个交易对的深度数据 DataFrame，包含 top5 档买卖盘，索引为时间戳
        返回: signals_df，index 为 symbol，columns 包括:
          - action: 'place_bid'/'place_ask'/'buy'/'sell'/'place_close'/'cancel_bid'/'cancel_ask'/'close'
          - price: 信号价格，可选
          - reason: 触发理由
          - position_size: 开仓比例或规模，可选
        """
        signals = []

        for symbol, ob in market_data.items():
            self._init_state(symbol)
            st = self.state[symbol]
            now = ob.index[-1]

            # 1. 强制平仓: 持仓超时或超规模
            if st["position"] != 0:
                hold = now - st["entry_time"]
                if hold > self.max_holding or abs(st["size"]) >= self.max_notional:
                    signals.append(
                        {
                            "symbol": symbol,
                            "action": "close",
                            "price": None,
                            "reason": "inventory_risk",
                            "position_size": 1.0,
                        }
                    )
                    # 重置状态
                    self.state[symbol] = {
                        "position": 0,
                        "entry_price": None,
                        "entry_time": None,
                        "pending": [],
                        "size": 0.0,
                    }
                    continue

            # 2. 无持仓，且无挂单，满足价差时挂两边 Maker 单
            if st["position"] == 0 and not st["pending"]:
                sp = self._spread(ob)
                if sp >= self.entry_spread:
                    mid = self._mid_price(ob)
                    bid_p = mid * (1 - self.delta)
                    ask_p = mid * (1 + self.delta)
                    st["pending"] = [("buy", bid_p), ("sell", ask_p)]
                    signals.append(
                        {
                            "symbol": symbol,
                            "action": "place_bid",
                            "price": bid_p,
                            "reason": "maker_entry",
                        }
                    )
                    signals.append(
                        {
                            "symbol": symbol,
                            "action": "place_ask",
                            "price": ask_p,
                            "reason": "maker_entry",
                        }
                    )

            # 3. 检查挂单被动成交 (被动成交视为入场)
            for order in st["pending"][:]:
                typ, p = order
                if (typ == "buy" and ob["ask"].iloc[0] <= p) or (
                    typ == "sell" and ob["bid"].iloc[0] >= p
                ):
                    # 入场信号
                    signals.append(
                        {
                            "symbol": symbol,
                            "action": "buy" if typ == "buy" else "sell",
                            "price": p,
                            "reason": "entry_fill",
                        }
                    )
                    st["position"] = 1 if typ == "buy" else -1
                    st["entry_price"] = p
                    st["entry_time"] = now
                    st["size"] = self.max_notional
                    # 取消另一侧挂单
                    opp = "cancel_ask" if typ == "buy" else "cancel_bid"
                    signals.append(
                        {
                            "symbol": symbol,
                            "action": opp,
                            "price": None,
                            "reason": "cancel_opposite",
                        }
                    )
                    st["pending"].clear()
                    break

            # 4. 已持仓，处理平仓逻辑
            if st["position"] != 0:
                # 跳过资金费套利窗口
                if self._in_funding_window(symbol, now):
                    continue

                mid = self._mid_price(ob)
                diff = (mid - st["entry_price"]) / st["entry_price"] * st["position"]

                # 4.1 目标价差平仓
                if diff >= self.exit_spread:
                    target_p = st["entry_price"] * (
                        1 + self.exit_spread * st["position"]
                    )
                    signals.append(
                        {
                            "symbol": symbol,
                            "action": "place_close",
                            "price": target_p,
                            "reason": "profit_target",
                        }
                    )
                    # 2 秒未成交则市价平仓
                    if (now - st["entry_time"]) > timedelta(seconds=2):
                        signals.append(
                            {
                                "symbol": symbol,
                                "action": "close",
                                "price": None,
                                "reason": "timeout_close",
                            }
                        )
                        self.state[symbol] = {
                            "position": 0,
                            "entry_price": None,
                            "entry_time": None,
                            "pending": [],
                            "size": 0.0,
                        }
                        continue

                # 4.2 反向滑点止损
                if diff <= -self.stop_loss:
                    signals.append(
                        {
                            "symbol": symbol,
                            "action": "close",
                            "price": None,
                            "reason": "stop_loss",
                        }
                    )
                    self.state[symbol] = {
                        "position": 0,
                        "entry_price": None,
                        "entry_time": None,
                        "pending": [],
                        "size": 0.0,
                    }
                    continue

                # 4.3 时间止盈
                if now - st["entry_time"] > self.time_exit:
                    signals.append(
                        {
                            "symbol": symbol,
                            "action": "close",
                            "price": None,
                            "reason": "time_exit",
                        }
                    )
                    self.state[symbol] = {
                        "position": 0,
                        "entry_price": None,
                        "entry_time": None,
                        "pending": [],
                        "size": 0.0,
                    }
                    continue

            # TODO: 资金费套利延迟平仓逻辑

        if signals:
            return pd.DataFrame(signals).set_index("symbol")
        return None
