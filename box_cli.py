import argparse
import sqlite3
import os
import json
import importlib
import traceback
import asyncio  # <--- 导入 asyncio
import sys  # 导入 sys
import uuid

# 导入本地模块
from strategies.mystrategy import MyStrategy
from trading.api import ExchangeInterface
from database.sqlite import SQLiteDB
from trading.trading_loop_future_fast import TradingLoop
from utils.logger import logger

if sys.platform == "win32":
    try:
        # 设置策略，让 asyncio 之后创建的循环都是 SelectorEventLoop
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        # 注意：这里不需要获取 logger，因为 logger 可能还没初始化
        logger.info("Set asyncio event loop policy to WindowsSelectorEventLoopPolicy.")
    except Exception as policy_e:
        # 如果设置失败，打印警告
        logger.warning(f"Failed to set asyncio event loop policy: {policy_e}")


def load_strategy(strategy_name: str) -> object:
    """动态加载并实例化策略类"""
    try:
        module_name = strategy_name.lower()
        module_path = f"strategies.{module_name}"
        logger.info(f"尝试从模块 {module_path} 加载策略类 {strategy_name}...")
        strategy_module = importlib.import_module(module_path)
        strategy_class = getattr(strategy_module, strategy_name)
        strategy_instance = strategy_class()
        if not isinstance(strategy_instance, object):
            raise TypeError(f"无法实例化策略 {strategy_name}")
        logger.info(f"成功加载并实例化策略: {strategy_name}")
        return strategy_instance
    except ModuleNotFoundError:
        logger.error(f"找不到策略模块: {module_path}.py")
        raise
    except AttributeError:
        logger.error(f"在模块 {module_path} 中找不到策略类: {strategy_name}")
        raise
    except Exception as e:
        logger.error(f"加载策略 {strategy_name} 失败: {e}")
        logger.error(traceback.format_exc())
        raise


def main():
    """主函数，解析参数并启动交易循环"""
    parser = argparse.ArgumentParser(description="命令行量化交易 Box")
    # --- 参数定义 (保持不变) ---
    parser.add_argument("--exchange", type=str, default="binance", help="交易所名称")
    parser.add_argument(
        "--market_type",
        type=str,
        default="future",
        choices=["spot", "future"],
        help="市场类型",
    )
    parser.add_argument("--testnet", action="store_true", help="使用测试网络")
    parser.add_argument("--api", type=str, required=True, help="API Key")
    parser.add_argument("--secret", type=str, required=True, help="Secret Key")
    parser.add_argument(
        "--task_name",
        type=str,
        default=f"{uuid.uuid4()}",
        help="任务名称",
    )
    parser.add_argument("--strategy", type=str, required=True, help="策略类名称")
    parser.add_argument(
        "--symbols", type=str, required=True, help="交易对列表 (逗号分隔)"
    )
    parser.add_argument(
        "--signal_interval", type=float, default=1.0, help="信号生成间隔(秒)"
    )
    parser.add_argument(
        "--price_check_interval", type=float, default=0.2, help="主循环检查间隔(秒)"
    )
    parser.add_argument("--leverage", type=int, default=10, help="合约杠杆倍数")
    parser.add_argument("--kline_limit", type=int, default=1000, help="K线数量")
    parser.add_argument(
        "--kline_interval_minutes", type=int, default=1, help="K线周期(分钟)"
    )
    parser.add_argument(
        "--position_size", type=float, default=0.1, help="开仓保证金比例"
    )
    args = parser.parse_args()

    task_id = None
    db: Optional[SQLiteDB] = None
    exchange_interface: Optional[ExchangeInterface] = None
    trading_loop: Optional[TradingLoop] = None

    # --- 异步主逻辑 ---
    async def async_main():
        nonlocal task_id, db, exchange_interface, trading_loop  # 允许修改外部变量
        try:
            # 1. 初始化数据库 (同步)
            db_path = "trading.db"
            db = SQLiteDB(db_path=db_path)
            logger.info(f"数据库初始化完成: {db_path}")

            # 2. 初始化交易所接口 (同步创建实例，异步加载数据)
            logger.info(f"创建交易所接口实例: {args.exchange}...")
            exchange_interface = ExchangeInterface(
                exchange_name=args.exchange,
                api=args.api,
                secret=args.secret,
                market_type=args.market_type,
                testnet=args.testnet,
            )
            # --- 异步初始化交易所 ---
            await exchange_interface.initialize()
            logger.info("交易所接口异步初始化完成")

            # 3. 加载策略实例 (同步)
            logger.info(f"加载策略: {args.strategy}")
            strategy_instance = load_strategy(args.strategy)
            logger.info("策略实例加载完成")

            # 4. 准备任务参数 (同步)
            task_parameters = {
                "exchange": args.exchange,
                "market_type": args.market_type,
                "testnet": args.testnet,
                "symbols": [s.strip() for s in args.symbols.split(",")],
                "strategy_name": args.strategy,
                "signal_interval": args.signal_interval,
                "price_check_interval": args.price_check_interval,
                "leverage": args.leverage,
                "kline_limit": args.kline_limit,
                "kline_interval_minutes": args.kline_interval_minutes,
                "position_size": args.position_size,
            }
            parameters_json = json.dumps(task_parameters)
            logger.debug(f"任务参数: {parameters_json}")

            # 5. 创建数据库任务记录 (同步)
            task_id = db.create_task(
                task_name=args.task_name, parameters=parameters_json
            )
            if task_id is None:
                raise RuntimeError("无法在数据库中创建任务记录 (可能任务名已存在)")
            logger.info(f"数据库任务已创建, Task ID: {task_id}, 名称: {args.task_name}")

            # 6. 初始化 TradingLoop (同步)
            trading_loop = TradingLoop(
                strategy=strategy_instance,
                exchange_interface=exchange_interface,
                db_instance=db,
            )
            logger.info("TradingLoop 初始化完成")

            # 7. 运行异步 TradingLoop
            logger.info(f"启动异步交易循环, Task ID: {task_id}...")
            await trading_loop.run(task_id)  # <--- await 异步 run 方法

            logger.info(f"交易循环正常结束, Task ID: {task_id}")

        except Exception as e:
            logger.error(f"运行 async_main 时发生严重错误: {e}")
            logger.error(traceback.format_exc())
            # 尝试更新任务状态为 error (同步)
            if task_id is not None and db:
                try:
                    db.update_task_status(
                        task_id, "error", result=f"Error in async_main: {e}"
                    )
                    logger.info(f"已将任务 {task_id} 状态更新为 error")
                except Exception as db_e:
                    logger.error(f"更新任务 {task_id} 状态为 error 时失败: {db_e}")
        finally:
            # --- 资源清理 ---
            if (
                trading_loop
                and hasattr(trading_loop, "_shutdown")
                and asyncio.iscoroutinefunction(trading_loop._shutdown)
            ):
                logger.info("确保 TradingLoop 已关闭...")
                # await trading_loop._shutdown() # run 结束时会调用，这里不需要重复
            elif (
                exchange_interface
                and hasattr(exchange_interface, "close")
                and asyncio.iscoroutinefunction(exchange_interface.close)
            ):
                logger.info("尝试关闭交易所接口...")
                await exchange_interface.close()  # 如果 run 提前退出，需要关闭

            if db:
                logger.info("关闭数据库连接...")
                db.close()

    # --- 启动主异步函数 ---
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，程序即将退出...")
    except Exception as final_e:
        logger.critical(f"程序顶层捕获到未处理异常: {final_e}")
        logger.critical(traceback.format_exc())


if __name__ == "__main__":
    # Windows 事件循环策略设置 (可以放在这里，如果 api.py 的导入有问题)
    # if sys.platform == 'win32':
    #      try:
    #          asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    #          logger.debug("Windows event loop policy set.")
    #      except Exception as policy_e:
    #          logger.warning(f"Failed to set Windows event loop policy: {policy_e}")
    main()
