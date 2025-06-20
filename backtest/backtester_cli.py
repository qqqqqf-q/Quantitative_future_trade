# future_trade/backtest/backtester_cli.py

import argparse
import os
import sys
import importlib
import traceback
import asyncio
import pandas as pd
import time

# 动态调整 Python 路径，以便能找到项目根目录下的模块
# 获取当前文件所在的目录 (future_trade/backtest)
current_dir = os.path.dirname(os.path.abspath(__file__))
# 获取项目根目录 (future_trade)
project_root = os.path.dirname(current_dir)
# 将项目根目录添加到 sys.path
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 现在可以导入项目模块了
from utils.logger import logger
from strategies.mystrategy import MyStrategy  # 导入你的策略类或基类
from backtest.engine import BacktestEngine
from backtest.results import print_metrics, generate_report_html


def load_strategy(strategy_name: str) -> object:
    """动态加载并实例化策略类 (类似 box_cli.py 中的逻辑)"""
    try:
        # 假设策略都在 strategies 目录下，且文件名与类名小写一致
        module_name = strategy_name.lower()
        module_path = f"strategies.{module_name}"
        logger.info(f"尝试从模块 {module_path} 加载策略类 {strategy_name}...")
        strategy_module = importlib.import_module(module_path)
        strategy_class = getattr(strategy_module, strategy_name)
        strategy_instance = strategy_class()  # 调用构造函数实例化
        # 可以添加类型检查，确保它是 MyStrategy 的子类或实现了特定接口
        # if not isinstance(strategy_instance, BaseStrategy):
        #     raise TypeError(...)
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


async def main():
    parser = argparse.ArgumentParser(description="期货交易策略回测工具")

    # --- 核心参数 ---
    parser.add_argument(
        "--strategy",
        type=str,
        required=True,
        help="要回测的策略类名称 (例如 MyStrategy)",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        required=True,
        help="要回测的交易对列表，逗号分隔 (例如 'BTC/USDT,ETH/USDT')",
    )
    parser.add_argument(
        "--start-date", type=str, required=True, help="回测开始日期 (格式: YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date", type=str, required=True, help="回测结束日期 (格式: YYYY-MM-DD)"
    )
    parser.add_argument(
        "--timeframe",
        type=str,
        default="1h",
        help="K线时间周期 (例如 '1m', '5m', '1h', '1d')",
    )
    parser.add_argument(
        "--initial-capital", type=float, default=10000.0, help="初始资金"
    )
    parser.add_argument("--leverage", type=int, default=10, help="模拟杠杆倍数")
    parser.add_argument(
        "--commission",
        type=float,
        default=0.0004,
        help="单边手续费率 (例如 0.0004 表示 0.04%%)",
    )
    parser.add_argument(
        "--slippage",
        type=float,
        default=0.0005,
        help="模拟滑点百分比 (例如 0.0005 表示 0.05%%)",
    )

    # --- 数据相关参数 ---
    parser.add_argument(
        "--db-path",
        type=str,
        default="trading.db",
        help="SQLite数据库文件路径 (相对于项目根目录)",
    )
    parser.add_argument(
        "--fetch-missing",
        action="store_true",
        help="如果数据库缺少数据，尝试从交易所获取",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default="binance",
        help="用于获取数据的交易所名称 (如果需要 fetch-missing)",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=os.environ.get("BACKTEST_API_KEY"),
        help="交易所 API Key (用于获取数据，可使用环境变量 BACKTEST_API_KEY)",
    )
    parser.add_argument(
        "--secret-key",
        type=str,
        default=os.environ.get("BACKTEST_SECRET_KEY"),
        help="交易所 Secret Key (用于获取数据，可使用环境变量 BACKTEST_SECRET_KEY)",
    )
    parser.add_argument("--testnet", action="store_true", help="使用测试网获取数据")

    # --- 输出参数 ---
    parser.add_argument(
        "--plot", action="store_true", help="生成并保存回测结果的 HTML 报告和图表"
    )
    parser.add_argument(
        "--report-dir", type=str, default="backtest_reports", help="HTML报告的输出目录"
    )
    parser.add_argument(
        "--proxy-url",  # <--- 新增代理参数
        type=str,
        default=None,
        help="用于交易所数据获取的代理 URL (例如 'http://127.0.0.1:1080' 或 'socks5://user:pass@host:port')",
    )
    args = parser.parse_args()

    # --- 参数处理 ---
    symbols_list = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols_list:
        logger.error("必须提供至少一个有效的交易对。")
        sys.exit(1)

    # 调整 db_path 为相对于项目根目录的路径
    db_full_path = os.path.join(project_root, args.db_path)
    logger.info(f"使用的数据库路径: {db_full_path}")
    # 调整 report_dir 为相对于项目根目录的路径
    report_full_dir = os.path.join(project_root, args.report_dir)

    # 检查获取数据所需的参数
    fetch_enabled = args.fetch_missing
    if fetch_enabled and not all([args.exchange, args.api_key, args.secret_key]):
        logger.warning(
            "开启了 --fetch-missing 但未提供完整的 --exchange, --api-key, --secret-key。将禁用数据获取。"
        )
        fetch_enabled = False

    # --- 加载策略 ---
    try:
        strategy_instance = load_strategy(args.strategy)
    except Exception:
        logger.error("策略加载失败，退出程序。")
        sys.exit(1)

    # --- 创建并运行回测引擎 ---
    logger.info("创建回测引擎...")
    engine = BacktestEngine(
        strategy_instance=strategy_instance,
        symbols=symbols_list,
        start_date=args.start_date,
        end_date=args.end_date,
        timeframe=args.timeframe,
        initial_capital=args.initial_capital,
        leverage=args.leverage,
        commission_rate=args.commission,
        slippage_pct=args.slippage,
        db_path=db_full_path,
        exchange_name=args.exchange if fetch_enabled else None,
        api_key=args.api_key if fetch_enabled else None,
        secret_key=args.secret_key if fetch_enabled else None,
        testnet=args.testnet,
        fetch_missing_data=fetch_enabled,
        proxy_url=args.proxy_url,  # 传递代理参数
    )

    logger.info(f"开始回测策略 '{args.strategy}'...")
    start_time = time.time()
    results = await engine.run()  # 运行异步回测
    end_time = time.time()
    logger.info(f"回测完成，总耗时: {end_time - start_time:.2f} 秒。")

    # --- 处理并显示结果 ---
    if (
        results
        and "metrics" in results
        and isinstance(results["metrics"], dict)
        and "错误" not in results["metrics"]
    ):
        print_metrics(results["metrics"])

        if args.plot:
            logger.info("生成回测报告...")
            generate_report_html(
                equity_curve=results["equity_curve"],
                trades=results["trades"],  # 传递 trades DataFrame
                strategy_name=args.strategy,  # 传递策略名称
                output_dir=report_full_dir,
            )

        # 可选：保存详细交易记录到 CSV
        # if not results['trades'].empty:
        #     trades_csv_path = os.path.join(report_full_dir, f"{args.strategy}_trades_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv")
        #     results['trades'].to_csv(trades_csv_path)
        #     logger.info(f"交易记录已保存到: {trades_csv_path}")

    elif results and "error" in results:
        logger.error(f"回测执行失败: {results['error']}")
    else:
        logger.error("回测未产生有效结果。")


if __name__ == "__main__":
    # Windows 事件循环策略设置 (如果需要，用于 asyncio)
    if sys.platform == "win32":
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            logger.debug("Windows event loop policy set.")
        except Exception as policy_e:
            logger.warning(f"Failed to set Windows event loop policy: {policy_e}")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，程序即将退出...")
    except Exception as final_e:
        logger.critical(f"程序顶层捕获到未处理异常: {final_e}")
        logger.critical(traceback.format_exc())
        sys.exit(1)  # 异常退出
    finally:
        logger.info("回测程序执行完毕。")
