import sqlite3
from datetime import datetime
from typing import List, Dict, Optional, Any
import pandas as pd
import json  # 用于处理 JSON 字符串
import time  # 用于 get_recent_orders

# 建议将 logger 从 utils 导入
from utils.logger import logger

# 默认数据库路径（如果调用者不提供）
DEFAULT_DB_PATH = "./database/trading.db"


class SQLiteDB:
    def __init__(
        self,
        db_path: str = DEFAULT_DB_PATH,
        check_same_thread: bool = True,  # 主线程使用 True，子线程使用 False
    ):
        """初始化数据库连接"""
        self.db_path = db_path
        self.check_same_thread = check_same_thread
        self.conn = None
        self.cursor = None
        self._connect()  # 建立连接
        self._create_tables()  # 确保表存在

    def _connect(self):
        """建立数据库连接"""
        try:
            # 允许线程模型里选择是否校验同线程
            self.conn = sqlite3.connect(
                self.db_path, check_same_thread=self.check_same_thread
            )
            # 设置 Row Factory 以便 fetchall 返回字典列表
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            logger.info(
                f"数据库连接已建立: {self.db_path} (check_same_thread={self.check_same_thread})"
            )
        except Exception as e:
            logger.error(f"连接数据库 {self.db_path} 失败: {e}")
            raise  # 抛出异常，让调用者知道连接失败

    def _create_tables(self):
        """创建所有需要的数据库表"""
        if not self.conn or not self.cursor:
            logger.error("数据库未连接，无法创建表")
            return
        try:
            # K线数据表 (全局共享，无 task_id)
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS klines (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    timestamp INTEGER NOT NULL, -- K线开始时间的毫秒时间戳
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    UNIQUE(symbol, timeframe, timestamp) -- 唯一性约束
                )
                """
            )

            # 交易历史表 (存储实际成交记录/Fills)
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER, -- 关联的任务 ID (可选)
                    order_id TEXT,    -- 交易所订单 ID
                    trade_id TEXT UNIQUE, -- 交易所成交 ID (如果可用，确保唯一)
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL, -- 'buy' or 'sell'
                    price REAL NOT NULL,
                    amount REAL NOT NULL,
                    cost REAL NOT NULL, -- 成交额 (price * amount)
                    fee REAL,         -- 手续费金额
                    fee_currency TEXT,-- 手续费币种
                    timestamp INTEGER NOT NULL, -- 成交时间的毫秒时间戳
                    is_maker BOOLEAN,   -- 是否是 Maker 成交 (如果交易所提供)
                    raw_data TEXT     -- 原始成交数据 (JSON)
                )
                """
            )
            # 为 trade_history 添加索引以提高查询效率
            self.cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_trade_history_symbol_time ON trade_history (symbol, timestamp);"
            )
            self.cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_trade_history_order_id ON trade_history (order_id);"
            )
            self.cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_trade_history_task_id ON trade_history (task_id);"
            )

            # 订单表 (存储尝试下的订单信息)
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL, -- 关联的任务 ID
                    client_order_id TEXT UNIQUE, -- 客户端自定义订单 ID (如果使用)
                    exchange_order_id TEXT,   -- 交易所返回的订单 ID
                    symbol TEXT NOT NULL,
                    order_type TEXT NOT NULL,   -- 'market', 'limit', 'close_position', etc.
                    side TEXT NOT NULL,         -- 'buy' or 'sell' (or 'close')
                    requested_price REAL,     -- 请求的价格 (限价单)
                    requested_amount REAL,    -- 请求的数量
                    requested_cost REAL,      -- 请求的成本 (市价买单)
                    position_size REAL,       -- 请求的仓位比例 (如适用)
                    status TEXT NOT NULL,       -- 订单状态 ('new', 'submitted', 'filled', 'partially_filled', 'canceled', 'rejected', 'error', 'no_position')
                    timestamp INTEGER NOT NULL, -- 下单尝试时间的毫秒时间戳
                    exchange_response TEXT,   -- 交易所返回的原始信息 (JSON)
                    error_message TEXT        -- 如果下单失败的错误信息
                )
                """
            )
            # 为 orders 表添加索引
            self.cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_orders_task_id ON orders (task_id);"
            )
            self.cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_orders_symbol_time ON orders (symbol, timestamp);"
            )
            self.cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);"
            )
            self.cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_orders_exchange_id ON orders (exchange_order_id);"
            )

            # 任务表
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_name TEXT NOT NULL UNIQUE, -- 任务名称应该是唯一的
                    status TEXT NOT NULL,         -- 'pending', 'running', 'stopping', 'stop', 'completed', 'error'
                    created_at INTEGER NOT NULL,  -- 创建时间的毫秒时间戳
                    started_at INTEGER,         -- 开始时间的毫秒时间戳
                    completed_at INTEGER,       -- 完成/停止时间的毫秒时间戳
                    parameters TEXT,            -- 任务参数 (JSON string)
                    result TEXT                 -- 任务结果或错误信息
                )
                """
            )
            # 为 tasks 表添加索引
            self.cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks (status);"
            )
            self.cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks (created_at);"
            )

            # 账户状态快照表
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS account_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,       -- 关联的任务 ID
                    timestamp INTEGER NOT NULL,     -- 快照时间的毫秒时间戳
                    total_balance_usdt REAL,      -- 账户总价值 (估算, USDT)
                    free_balance_usdt REAL,       -- 可用保证金/余额 (USDT)
                    used_balance_usdt REAL,       -- 已用保证金/余额 (USDT)
                    total_unrealized_pnl REAL,    -- 总未实现盈亏 (期货)
                    positions_data TEXT,          -- 持仓详细信息 (JSON string)
                    balance_details TEXT          -- 完整余额信息 (JSON string, 可选)
                )
                """
            )
            # 为 account_status 添加索引
            self.cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_account_status_task_id_time ON account_status (task_id, timestamp);"
            )

            self.conn.commit()
            logger.info("数据库表结构检查/创建完成")
        except Exception as e:
            logger.error(f"创建数据库表失败: {e}")
            if self.conn:
                self.conn.rollback()  # 出错时回滚

    # ========== K线数据操作 (全局共享) ==========
    def insert_klines(self, symbol: str, timeframe: str, klines: List[List[Any]]):
        """
        批量插入K线数据，确保去重并按时间升序排列。
        K线数据是全局共享的，不与特定任务关联。

        :param symbol: 交易对，例如 'BTC/USDT'
        :param timeframe: K线周期，例如 '1m', '1h'
        :param klines: K线数据列表，格式 [[timestamp_ms, open, high, low, close, volume], ...]
        """
        if not self.conn or not self.cursor:
            return False
        if not klines:
            return True  # 没有数据就直接返回成功

        # 准备插入的数据元组列表
        # 确保时间戳是整数毫秒
        kline_tuples = [
            (
                symbol,
                timeframe,
                int(k[0]),  # timestamp_ms
                k[1],  # open
                k[2],  # high
                k[3],  # low
                k[4],  # close
                k[5],  # volume
            )
            for k in klines
            if len(k) >= 6  # 确保每条K线数据完整
        ]

        if not kline_tuples:
            logger.warning(f"[{symbol}|{timeframe}] 提供的 K 线数据格式不正确或为空")
            return False

        try:
            # 使用 INSERT OR IGNORE 避免插入重复数据 (基于 UNIQUE 约束)
            self.cursor.executemany(
                """
                INSERT OR IGNORE INTO klines
                (symbol, timeframe, timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                kline_tuples,
            )
            self.conn.commit()
            # logger.debug(f"[{symbol}|{timeframe}] 插入或忽略了 {len(kline_tuples)} 条 K 线数据")
            return True
        except Exception as e:
            logger.error(f"[{symbol}|{timeframe}] 插入K线数据失败: {e}")
            self.conn.rollback()
            return False

    def get_klines(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 1000,
        end_timestamp_ms: Optional[int] = None,  # 可选：获取指定时间点之前的 K 线
    ) -> pd.DataFrame:
        """
        获取指定交易对和周期的K线数据，返回按时间升序排列的DataFrame。

        :param symbol: 交易对
        :param timeframe: K线周期
        :param limit: 获取的最大 K 线数量
        :param end_timestamp_ms: 获取结束时间戳（毫秒）之前的K线，默认为None（获取最新的）
        :return: K线数据的 Pandas DataFrame，如果无数据则返回空 DataFrame
        """
        if not self.conn or not self.cursor:
            return pd.DataFrame()

        query = """
        SELECT timestamp, open, high, low, close, volume
        FROM klines
        WHERE symbol = ? AND timeframe = ?
        """
        params = [symbol, timeframe]

        if end_timestamp_ms is not None:
            query += " AND timestamp <= ?"
            params.append(end_timestamp_ms)

        # 获取最新的 K 线，所以按时间戳降序排序，然后取 limit 条
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        try:
            # 执行查询
            df = pd.read_sql_query(query, self.conn, params=tuple(params))

            if df.empty:
                # logger.debug(f"数据库中未找到 {symbol} {timeframe} 的 K 线数据")
                return pd.DataFrame()

            # 将时间戳转换为 DatetimeIndex (UTC)
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)

            # 因为我们是 DESC 获取的，需要反转顺序变为升序
            df.sort_index(
                ascending=False, inplace=True
            )  # 先按降序索引（pandas读取后默认是0,1,2...）
            df.sort_values(by="timestamp", ascending=True, inplace=True)  # 再按时间升序

            # 设置时间戳为索引
            df.set_index("timestamp", inplace=True)

            # logger.debug(f"从数据库获取到 {len(df)} 条 {symbol} {timeframe} K 线数据")
            return df

        except Exception as e:
            logger.error(f"获取 {symbol} {timeframe} K 线数据失败: {e}")
            return pd.DataFrame()

    def maintain_klines_limit(self, symbol: str, timeframe: str, limit: int = 1000):
        """
        维护K线数据数量，仅保留最新的 'limit' 条数据。

        :param symbol: 交易对
        :param timeframe: K线周期
        :param limit: 要保留的数据条数
        """
        if not self.conn or not self.cursor:
            return False
        try:
            # SQLite 不支持直接 DELETE ... ORDER BY ... LIMIT ... OFFSET
            # 我们需要先找出要删除的记录的 ID
            # 选出除了最新的 limit 条之外的所有记录的 id
            self.cursor.execute(
                """
                DELETE FROM klines
                WHERE id IN (
                    SELECT id FROM klines
                    WHERE symbol = ? AND timeframe = ?
                    ORDER BY timestamp DESC
                    LIMIT -1 OFFSET ?
                )
                """,
                (symbol, timeframe, limit),
            )
            deleted_rows = self.cursor.rowcount
            self.conn.commit()
            if deleted_rows > 0:
                logger.debug(
                    f"[{symbol}|{timeframe}] 维护 K 线数据：删除了 {deleted_rows} 条旧记录，保留最新 {limit} 条"
                )
            return True
        except Exception as e:
            logger.error(f"[{symbol}|{timeframe}] 维护K线数据失败: {e}")
            self.conn.rollback()
            return False

    # ========== 订单操作 (与任务关联) ==========
    def save_order(
        self,
        task_id: int,
        symbol: str,
        order_type: str,  # 例如 'buy', 'sell', 'close'
        side: str,  # 例如 'buy', 'sell'
        requested_price: Optional[float] = None,
        requested_amount: Optional[float] = None,
        requested_cost: Optional[float] = None,
        position_size: Optional[float] = None,
        order_result: Optional[Dict] = None,  # 交易所返回的订单信息
        status: str = "unknown",  # 订单尝试的状态
        error_message: Optional[str] = None,
    ):
        """
        保存订单尝试的信息到数据库。

        :param task_id: 关联的任务 ID
        :param symbol: 交易对
        :param order_type: 尝试的订单类型 ('buy', 'sell', 'close_position')
        :param side: 实际执行的方向 ('buy', 'sell')
        :param requested_price: 请求的价格 (限价单)
        :param requested_amount: 请求的数量
        :param requested_cost: 请求的成本 (市价买单)
        :param position_size: 请求的仓位比例
        :param order_result: 交易所返回的原始结果字典
        :param status: 订单尝试的结果状态 ('submitted', 'error', 'no_position', etc.)
        :param error_message: 如果失败，记录错误信息
        """
        if not self.conn or not self.cursor:
            return False
        timestamp = int(time.time() * 1000)
        exchange_order_id = order_result.get("id") if order_result else None
        exchange_response = json.dumps(order_result) if order_result else None

        # 如果是 'no_position' 状态，特殊处理
        if status == "no_position":
            side = "none"
            requested_price = 0.0
            requested_amount = 0.0
            requested_cost = 0.0

        try:
            self.cursor.execute(
                """
                INSERT INTO orders
                (task_id, exchange_order_id, symbol, order_type, side,
                 requested_price, requested_amount, requested_cost, position_size,
                 status, timestamp, exchange_response, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    exchange_order_id,
                    symbol,
                    order_type,
                    side,
                    requested_price,
                    requested_amount,
                    requested_cost,
                    position_size,
                    status,
                    timestamp,
                    exchange_response,
                    error_message,
                ),
            )
            self.conn.commit()
            logger.info(
                f"[Task {task_id}|{symbol}] 订单尝试已记录: 类型={order_type}, 状态={status}, ExchangeID={exchange_order_id or 'N/A'}"
            )
            return True
        except Exception as e:
            logger.error(f"[Task {task_id}|{symbol}] 保存订单尝试失败: {e}")
            self.conn.rollback()
            return False

    def get_recent_orders(
        self, task_id: int, symbol: Optional[str] = None, limit: int = 10
    ) -> List[Dict]:
        """
        获取指定任务最近的订单尝试记录。

        :param task_id: 任务 ID
        :param symbol: 可选，过滤特定交易对
        :param limit: 返回的最大记录数
        :return: 订单记录字典列表
        """
        if not self.conn or not self.cursor:
            return []
        query = "SELECT * FROM orders WHERE task_id = ?"
        params = [task_id]

        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        try:
            self.cursor.execute(query, tuple(params))
            rows = self.cursor.fetchall()
            return [dict(row) for row in rows]  # 将 sqlite3.Row 转换为字典
        except Exception as e:
            logger.error(f"[Task {task_id}] 获取最近订单失败: {e}")
            return []

    # ========== 交易历史操作 (成交记录/Fills) ==========
    def insert_trade(self, task_id: Optional[int], trade: Dict):
        """
        插入单条交易成交记录 (Fill)。

        :param task_id: 关联的任务 ID (可选)
        :param trade: 包含成交信息的字典，应包含:
                      'id'(trade_id), 'order'(order_id), 'timestamp', 'symbol',
                      'side', 'price', 'amount', 'cost', 'fee'{'cost', 'currency'},
                      'makerOrTaker' (可选)
        """
        if not self.conn or not self.cursor:
            return False
        try:
            fee_cost = trade.get("fee", {}).get("cost")
            fee_currency = trade.get("fee", {}).get("currency")
            is_maker = (
                trade.get("takerOrMaker") == "maker"
                if "takerOrTaker" in trade
                else None
            )
            timestamp_ms = trade.get("timestamp")  # ccxt 返回的是毫秒

            self.cursor.execute(
                """
                INSERT OR IGNORE INTO trade_history
                (task_id, order_id, trade_id, symbol, side, price, amount, cost,
                 fee, fee_currency, timestamp, is_maker, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    trade.get("order"),
                    trade.get("id"),
                    trade["symbol"],
                    trade["side"],
                    trade["price"],
                    trade["amount"],
                    trade["cost"],
                    fee_cost,
                    fee_currency,
                    timestamp_ms,
                    is_maker,
                    json.dumps(trade),  # 保存原始数据
                ),
            )
            self.conn.commit()
            # logger.debug(f"[Task {task_id or 'N/A'}|{trade['symbol']}] 成交记录已插入: TradeID={trade.get('id')}")
            return True
        except Exception as e:
            logger.error(
                f"[Task {task_id or 'N/A'}] 插入成交记录失败: {e}, 数据: {trade}"
            )
            self.conn.rollback()
            return False

    def get_trades(
        self,
        task_id: Optional[int] = None,
        symbol: Optional[str] = None,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
        limit: Optional[int] = 100,
    ) -> List[Dict]:
        """
        获取交易历史 (成交记录/Fills)。

        :param task_id: 可选，过滤特定任务
        :param symbol: 可选，过滤特定交易对
        :param start_time_ms: 可选，开始时间戳 (毫秒)
        :param end_time_ms: 可选，结束时间戳 (毫秒)
        :param limit: 返回的最大记录数
        :return: 成交记录字典列表
        """
        if not self.conn or not self.cursor:
            return []
        query = "SELECT * FROM trade_history WHERE 1=1"
        params = []

        if task_id is not None:
            query += " AND task_id = ?"
            params.append(task_id)
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
        if start_time_ms:
            query += " AND timestamp >= ?"
            params.append(start_time_ms)
        if end_time_ms:
            query += " AND timestamp <= ?"
            params.append(end_time_ms)

        query += " ORDER BY timestamp DESC"

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        try:
            self.cursor.execute(query, tuple(params))
            rows = self.cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"获取交易历史失败: {e}")
            return []

    # ========== 任务操作 ==========
    def create_task(self, task_name: str, parameters: str) -> Optional[int]:
        """
        创建新任务。

        :param task_name: 任务名称
        :param parameters: 任务参数的 JSON 字符串
        :return: 新创建任务的 ID，如果失败则返回 None
        """
        if not self.conn or not self.cursor:
            return None
        timestamp = int(datetime.now().timestamp() * 1000)
        try:
            self.cursor.execute(
                """
                INSERT INTO tasks
                (task_name, status, created_at, parameters)
                VALUES (?, ?, ?, ?)
                """,
                (task_name, "pending", timestamp, parameters),
            )
            self.conn.commit()
            task_id = self.cursor.lastrowid
            logger.info(f"任务 '{task_name}' 已创建, Task ID: {task_id}")
            return task_id
        except sqlite3.IntegrityError:
            logger.error(f"创建任务失败：任务名称 '{task_name}' 已存在")
            self.conn.rollback()
            return None
        except Exception as e:
            logger.error(f"创建任务 '{task_name}' 失败: {e}")
            self.conn.rollback()
            return None

    def update_task_status(
        self, task_id: int, status: str, result: Optional[str] = None
    ):
        """
        更新任务状态和时间戳。

        :param task_id: 任务 ID
        :param status: 新状态 ('running', 'stopping', 'stop', 'completed', 'error')
        :param result: 任务结果或错误信息 (可选)
        :return: 是否成功更新
        """
        if not self.conn or not self.cursor:
            return False
        timestamp = int(datetime.now().timestamp() * 1000)
        update_fields = {"status": status}
        if status == "running":
            update_fields["started_at"] = timestamp
        elif status in ["completed", "stop", "error"]:
            update_fields["completed_at"] = timestamp
        if result is not None:
            update_fields["result"] = result

        set_clause = ", ".join([f"{key} = ?" for key in update_fields.keys()])
        params = list(update_fields.values())
        params.append(task_id)

        query = f"UPDATE tasks SET {set_clause} WHERE id = ?"

        try:
            self.cursor.execute(query, tuple(params))
            self.conn.commit()
            logger.info(f"任务 {task_id} 状态已更新为: {status}")
            return True
        except Exception as e:
            logger.error(f"更新任务 {task_id} 状态为 {status} 失败: {e}")
            self.conn.rollback()
            return False

    def get_task(self, task_id: int) -> Optional[Dict]:
        """
        获取任务详情。

        :param task_id: 任务 ID
        :return: 任务详情字典，如果任务不存在则返回 None
        """
        if not self.conn or not self.cursor:
            return None
        try:
            self.cursor.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
            row = self.cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"获取任务 {task_id} 详情失败: {e}")
            return None

    def get_task_status(self, task_id: int) -> Optional[str]:
        """
        快速获取任务状态。

        :param task_id: 任务 ID
        :return: 任务状态字符串，如果任务不存在则返回 None
        """
        if not self.conn or not self.cursor:
            return None
        try:
            self.cursor.execute("SELECT status FROM tasks WHERE id = ?", (task_id,))
            result = self.cursor.fetchone()
            return result["status"] if result else None
        except Exception as e:
            logger.error(f"获取任务 {task_id} 状态失败: {e}")
            return None

    def get_tasks(
        self,
        status: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Dict]:
        """
        获取任务列表。

        :param status: 可选的任务状态过滤
        :param limit: 返回的最大记录数
        :param offset: 分页偏移量
        :return: 任务字典列表
        """
        if not self.conn or not self.cursor:
            return []
        query = "SELECT * FROM tasks WHERE 1=1"
        params = []

        if status:
            query += " AND status = ?"
            params.append(status)

        query += " ORDER BY created_at DESC"  # 按创建时间降序

        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        if offset is not None:
            query += " OFFSET ?"
            params.append(offset)

        try:
            self.cursor.execute(query, tuple(params))
            rows = self.cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"获取任务列表失败: {e}")
            return []

    # ========== 账户状态操作 (与任务关联) ==========
    def save_account_status(
        self,
        task_id: int,
        balance_info: Dict,  # 包含 total_value_usdt, free, used 等的字典
        pnl_info: Dict,  # 包含 total unrealized PNL 和各持仓 PNL 的字典
        positions_info: Dict,  # 包含详细持仓信息的字典
    ):
        """
        保存账户状态快照信息。

        :param task_id: 关联的任务 ID
        :param balance_info: 交易所返回的余额信息处理后的字典
        :param pnl_info: 计算得到的盈亏信息字典
        :param positions_info: 交易所返回的持仓信息处理后的字典
        """
        if not self.conn or not self.cursor:
            return False
        timestamp = int(datetime.now().timestamp() * 1000)
        try:
            total_balance = balance_info.get("total_value_usdt", 0.0)
            # 假设 USDT 是主要保证金货币
            free_balance = balance_info.get("free", {}).get("USDT", 0.0)
            used_balance = balance_info.get("used", {}).get("USDT", 0.0)
            total_pnl = pnl_info.get("total", {}).get("unrealized_profit", 0.0)
            positions_json = json.dumps(positions_info)
            balance_details_json = json.dumps(
                balance_info.get("info", {})
            )  # 保存原始余额信息

            self.cursor.execute(
                """
                INSERT INTO account_status
                (task_id, timestamp, total_balance_usdt, free_balance_usdt, used_balance_usdt,
                 total_unrealized_pnl, positions_data, balance_details)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    timestamp,
                    total_balance,
                    free_balance,
                    used_balance,
                    total_pnl,
                    positions_json,
                    balance_details_json,
                ),
            )
            self.conn.commit()
            # logger.debug(f"[Task {task_id}] 账户状态快照已保存")
            return True
        except Exception as e:
            logger.error(f"[Task {task_id}] 保存账户状态失败: {e}")
            self.conn.rollback()
            return False

    # ========== 清理与关闭 ==========
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            try:
                self.conn.close()
                logger.info(f"数据库连接已关闭: {self.db_path}")
                self.conn = None
                self.cursor = None
            except Exception as e:
                logger.error(f"关闭数据库连接 {self.db_path} 时出错: {e}")

    def __del__(self):
        """析构时尝试关闭连接"""
        self.close()
