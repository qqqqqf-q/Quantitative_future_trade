import os
import sys
from loguru import logger
from typing import Optional

# 尝试相对导入 ConfigManager
try:
    from config.config import ConfigManager
except ImportError:
    # 如果相对导入失败，可能是直接运行此文件或 PYTHONPATH 问题
    # 尝试绝对导入（需要项目根目录在 PYTHONPATH 中）
    try:
        from config.config import ConfigManager
    except ImportError:
        # 如果还是失败，提供一个 fallback 或抛出错误
        print(
            "错误：无法导入 ConfigManager。请确保 PYTHONPATH 设置正确或使用相对导入。",
            file=sys.stderr,
        )

        # 定义一个假的 ConfigManager 以免完全崩溃
        class ConfigManager:
            def get(self, key, default=None):
                return default

        # raise ImportError("无法导入 ConfigManager")


class LoggerManager:
    def __init__(self):
        """
        初始化日志管理器，从配置管理器获取配置。
        """
        try:
            # 创建 ConfigManager 实例来获取配置
            # 注意：这可能导致循环导入，如果 ConfigManager 也导入了 logger
            # 更好的做法是将日志配置直接传入或使用更简单的配置加载方式
            config = ConfigManager()  # 假设 ConfigManager 此时可用
            log_dir = config.get("logging.log_dir", "logs")  # 使用默认值 'logs'
            log_level = config.get("logging.level", "INFO")
            log_level = "DEBUG"
            rotate_size = config.get("logging.max_file_size", "10 MB")
            keep_days = config.get("logging.retention_days", 7)
            log_to_console = config.get(
                "logging.console", True
            )  # 新增：是否输出到控制台
            log_to_file = config.get("logging.file", True)  # 新增：是否输出到文件

        except Exception as config_e:
            print(f"初始化 Logger 时加载配置失败: {config_e}", file=sys.stderr)
            log_dir = "logs"
            log_level = "INFO"
            rotate_size = "10 MB"
            keep_days = 7
            log_to_console = True
            log_to_file = True

        # 确保日志目录存在
        try:
            if log_to_file:
                os.makedirs(log_dir, exist_ok=True)
        except OSError as e:
            print(f"创建日志目录 '{log_dir}' 失败: {e}", file=sys.stderr)
            log_to_file = False  # 无法创建目录，则不记录到文件

        # 移除默认日志处理器
        logger.remove()

        # 控制台日志配置
        if log_to_console:
            logger.add(
                sys.stderr,
                level=log_level.upper(),  # 确保是大写
                format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "  # 添加毫秒
                "<level>{level: <8}</level> | "
                "<cyan>{thread.name: <15}</cyan> | "  # 显示线程名称
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                "<level>{message}</level>",
                enqueue=True,  # 异步写入，提高性能
            )

        # 文件日志配置
        if log_to_file:
            try:
                log_file_path = os.path.join(log_dir, "trading_bot_{time:YYYYMMDD}.log")
                logger.add(
                    log_file_path,
                    level=log_level.upper(),
                    rotation=rotate_size,
                    retention=f"{keep_days} days",
                    compression="zip",
                    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {thread.name: <15} | {name}:{function}:{line} | {message}",
                    encoding="utf-8",  # 指定编码
                    enqueue=True,  # 异步写入
                )
                print(f"日志将记录到: {log_file_path}")  # 启动时提示日志文件位置
            except Exception as file_log_e:
                print(f"添加文件日志处理器失败: {file_log_e}", file=sys.stderr)

        self.logger = logger
        self.logger.info("日志管理器初始化完成")  # 使用配置好的 logger 记录

    def get_logger(self):
        """获取配置好的 loguru logger 实例"""
        return self.logger

    # 以下 log_trade 和 log_error 方法是示例，实际使用中可以直接调用 logger.info/error 等
    def log_trade(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: float,
        order_id: Optional[str] = None,
        strategy: Optional[str] = None,
    ):
        """(示例) 记录交易执行日志"""
        log_message = (
            f"Trade Executed: "
            f"Symbol={symbol}, Side={side}, Amount={amount:.8f}, Price={price:.4f}"
        )
        if order_id:
            log_message += f", OrderID={order_id}"
        if strategy:
            log_message += f", Strategy={strategy}"
        self.logger.success(log_message)  # 使用 success 级别表示成功交易

    def log_error_detail(  # 重命名以区分 logger.error
        self,
        error_message: str,
        error_type: Optional[str] = None,
        details: Optional[dict] = None,
    ):
        """(示例) 记录带有额外信息的错误日志"""
        log_entry = error_message
        if error_type:
            log_entry += f" [Type: {error_type}]"
        if details:
            try:
                details_str = json.dumps(details, indent=2)
                log_entry += f"\nDetails:\n{details_str}"
            except TypeError:
                log_entry += f" Details: {details}"  # 如果无法序列化为 JSON

        self.logger.error(log_entry)


# --- 全局日志实例 ---
# 在模块加载时立即初始化日志管理器并获取 logger 实例
# 这样其他模块可以直接导入 logger 使用
try:
    logger_manager = LoggerManager()
    logger = logger_manager.get_logger()
except Exception as init_e:
    # 如果 LoggerManager 初始化失败，提供一个备用的基本 logger
    print(f"全局日志初始化失败: {init_e}", file=sys.stderr)
    logger.add(sys.stderr, level="DEBUG")  # 添加一个默认处理器
    logger = logger  # 保持 logger 可用
