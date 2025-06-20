# future_trade/backtest/results.py

import pandas as pd
import quantstats as qs
import matplotlib.pyplot as plt
import numpy as np
import os
from typing import Dict, List, Optional, Any
from utils.logger import logger  # 导入 logger 用于警告

# 确保 quantstats 使用 matplotlib 作为后端 (如果需要生成独立图片)
qs.extend_pandas()

# 设置 Matplotlib 支持中文显示 (根据你的系统选择合适的字体)
try:
    plt.rcParams["font.sans-serif"] = ["SimHei"]  # 例如 'SimHei' 或 'Microsoft YaHei'
    plt.rcParams["axes.unicode_minus"] = False  # 解决负号显示问题
except Exception as e:
    # logger.warning(f"设置中文字体失败: {e}。图表中的中文可能无法正常显示。")
    print(
        f"警告：设置中文字体失败: {e}。图表中的中文可能无法正常显示。"
    )  # 在 logger 可能还未完全配置好时用 print


def determine_years(returns: pd.Series) -> float:
    """
    计算收益率序列跨越的年数（按公历天数/365 计算）
    :param returns: Pandas Series，索引为 DatetimeIndex
    :return: 浮点型年数
    """
    if returns.empty or len(returns.index) < 2:
        return 0.0  # 如果数据点不足，返回 0 年
    start_date = returns.index.min()
    end_date = returns.index.max()
    days_diff = (end_date - start_date).days
    return days_diff / 365.0 if days_diff > 0 else 0.0  # 避免除以零或负数


def calculate_metrics(
    equity_curve: pd.Series, trades: pd.DataFrame, initial_capital: float
) -> Dict[str, Optional[Any]]:
    """
    使用 quantstats 计算回测性能指标，并基于平仓记录计算交易相关指标。
    增加了对 None 值和除零错误的更严格检查。

    :param equity_curve: 包含每日 (或每周期) 总权益的 Pandas Series，索引为时间戳。
    :param trades: 包含交易记录的 Pandas DataFrame (必须包含 'order_type' 和 'pnl' 列)。
    :param initial_capital: 初始资金。
    :return: 包含关键性能指标的字典。
    """
    metrics: Dict[str, Optional[Any]] = {}

    if equity_curve.empty:
        logger.error("权益曲线为空，无法计算指标。")
        return {"错误": "权益曲线为空，无法计算指标"}

    if not isinstance(equity_curve.index, pd.DatetimeIndex):
        try:
            equity_curve.index = pd.to_datetime(equity_curve.index)
            logger.info("权益曲线索引已转换为 DatetimeIndex。")
        except Exception as e:
            logger.error(f"无法将权益曲线索引转换为 DatetimeIndex: {e}")
            return {"错误": "权益曲线索引格式错误"}

    returns = equity_curve.pct_change().fillna(0)

    # --- 基础指标和准备 ---
    metrics = {
        "初始资金": initial_capital,
        "最终权益": equity_curve.iloc[-1],
        "总回报 (%)": None,
        "最大回撤 (%)": None,
        "年化回报 (%)": None,
        "年化波动率 (%)": None,
        "夏普比率 (年化)": None,
        "索提诺比率 (年化)": None,
        "卡玛比率": None,
    }

    # --- 基于权益曲线的指标 (QuantStats) ---
    data_points = len(returns)
    if data_points < 2:
        logger.warning("收益率序列数据点过少 (< 2)，部分指标无法计算。")
        # 只有最终权益和初始资金有效
        if initial_capital > 0:
            metrics["总回报 (%)"] = ((metrics["最终权益"] / initial_capital) - 1) * 100
        else:
            metrics["总回报 (%)"] = 0.0
        metrics["最大回撤 (%)"] = 0.0  # 无法计算回撤
    else:
        try:
            metrics["总回报 (%)"] = qs.stats.comp(returns) * 100
        except Exception as e:
            logger.warning(f"计算总回报失败: {e}")

        try:
            # 计算最大回撤，即使不能年化也计算
            max_dd = qs.stats.max_drawdown(equity_curve)  # qs 返回的是负数或0
            metrics["最大回撤 (%)"] = abs(max_dd) * 100  # 我们通常报告正数的回撤百分比
        except Exception as e:
            logger.warning(f"计算最大回撤失败: {e}")
            metrics["最大回撤 (%)"] = None  # 明确设为 None

        years = determine_years(returns)
        can_annualize = (
            years > 1e-6
        )  # 0.0 # (3 / 365.0) # 例如，至少需要几天的数据才能年化

        if not can_annualize:
            logger.warning("回测时间跨度过短，年化指标将为 N/A。")
        else:
            logger.info(f"回测周期约 {years:.2f} 年，计算年化指标...")
            # 使用 try-except 包裹每个可能出错的年化计算
            try:
                cagr = qs.stats.cagr(returns, periods=365)
                metrics["年化回报 (%)"] = cagr * 100
            except Exception as e:
                logger.warning(f"计算年化回报 (CAGR) 失败: {e}")

            try:
                # 假设 returns 是日度收益
                vol = qs.stats.volatility(returns, annualize=True, periods=365)
                metrics["年化波动率 (%)"] = vol * 100
            except Exception as e:
                logger.warning(f"计算年化波动率失败: {e}")

            try:
                sharpe = qs.stats.sharpe(returns, rf=0.0, annualize=True, periods=None)
                # Quantstats 可能返回 NaN 或 Inf，需要处理
                if np.isfinite(sharpe):
                    metrics["夏普比率 (年化)"] = sharpe
            except Exception as e:
                logger.warning(f"计算夏普比率失败: {e}")

            try:
                sortino = qs.stats.sortino(
                    returns, rf=0.0, annualize=True, periods=None
                )
                if np.isfinite(sortino):
                    metrics["索提诺比率 (年化)"] = sortino
            except Exception as e:
                logger.warning(f"计算索提诺比率失败: {e}")

            # 卡玛比率 = 年化回报 / 最大回撤绝对值
            # **进行除法前严格检查**
            cagr_val = metrics.get("年化回报 (%)")  # 这已经是 *100 的百分比
            max_dd_pct = metrics.get("最大回撤 (%)")  # 这是正的百分比

            if cagr_val is not None and max_dd_pct is not None:
                # 检查最大回撤是否接近于 0
                if abs(max_dd_pct) > 1e-6:  # 避免除以非常小的数或零
                    try:
                        # 注意：这里 CAGR 和 Max Drawdown 都是百分比，直接相除即可
                        calmar_ratio = cagr_val / max_dd_pct
                        if np.isfinite(calmar_ratio):
                            metrics["卡玛比率"] = calmar_ratio
                        else:
                            logger.warning("计算得到的卡玛比率不是有效数字 (NaN/inf)")
                    except ZeroDivisionError:
                        logger.warning("最大回撤为零，无法计算卡玛比率。")
                    except Exception as e:
                        logger.warning(f"计算卡玛比率时出错: {e}")
                else:
                    logger.warning("最大回撤为零或接近零，无法计算卡玛比率。")
            else:
                logger.warning("年化回报或最大回撤未能成功计算，无法计算卡玛比率。")

    # --- 基于交易记录的指标 ---
    # (这部分逻辑基本不变，但确保默认值为 None 或 0.0)
    metrics.update(
        {
            "完整交易次数": 0,
            "胜率 (%)": None,
            "平均盈利 ($)": None,
            "平均亏损 ($)": None,
            "盈亏比": None,
            "总净盈利 ($)": 0.0,
            "总操作次数": 0,
        }
    )

    if not trades.empty and "order_type" in trades.columns and "pnl" in trades.columns:
        metrics["总操作次数"] = len(trades)
        closing_trades = trades[
            trades["order_type"].str.contains("close", case=False, na=False)
        ].copy()
        total_closing_trades = len(closing_trades)
        metrics["完整交易次数"] = total_closing_trades

        if total_closing_trades > 0:
            total_net_profit = closing_trades["pnl"].sum()
            metrics["总净盈利 ($)"] = total_net_profit

            winning_closing_trades = closing_trades[closing_trades["pnl"] > 0]
            actual_losing_trades = closing_trades[closing_trades["pnl"] < 0]
            count_wins = len(winning_closing_trades)
            count_actual_losses = len(actual_losing_trades)
            metrics["盈利次数"] = count_wins
            metrics["亏损次数"] = count_actual_losses
            metrics["总亏损额 ($)"] = (
                actual_losing_trades["pnl"].sum() if count_actual_losses > 0 else 0.0
            )
            metrics["胜率 (%)"] = (count_wins / total_closing_trades) * 100

            avg_win = winning_closing_trades["pnl"].mean() if count_wins > 0 else 0.0
            # mean() 在空 Series 上返回 NaN，需要检查
            if pd.isna(avg_win):
                avg_win = 0.0
            metrics["平均盈利 ($)"] = avg_win

            avg_loss = (
                actual_losing_trades["pnl"].mean() if count_actual_losses > 0 else 0.0
            )
            if pd.isna(avg_loss):
                avg_loss = 0.0
            metrics["平均亏损 ($)"] = avg_loss  # 保持为负数或0

            avg_loss_abs = abs(avg_loss)
            if avg_loss_abs > 1e-9:  # 检查非零亏损
                # 确保 avg_win 是有效数字
                if avg_win is not None and np.isfinite(avg_win):
                    try:
                        profit_loss_ratio = avg_win / avg_loss_abs
                        if np.isfinite(profit_loss_ratio):
                            metrics["盈亏比"] = profit_loss_ratio
                        else:
                            logger.warning("计算得到的盈亏比不是有效数字 (NaN/inf)")
                    except Exception as e:
                        logger.warning(f"计算盈亏比时出错: {e}")
                else:
                    logger.warning("平均盈利无效，无法计算盈亏比。")
            elif avg_win > 1e-9:  # 无亏损但有盈利
                metrics["盈亏比"] = float("inf")
            else:  # 无亏损也无盈利
                metrics["盈亏比"] = None
        else:
            logger.warning("没有平仓交易记录，无法计算交易相关指标。")
    else:
        logger.warning("交易记录为空或格式不正确，无法计算交易相关指标。")

    # 清理 NaN 或 Inf 值，替换为 None 以便打印
    final_metrics = {}
    for key, value in metrics.items():
        if isinstance(value, (float, int)) and not np.isfinite(value):
            final_metrics[key] = None  # 将 NaN 和 Inf 替换为 None
        else:
            final_metrics[key] = value

    return final_metrics


def generate_report_html(
    equity_curve: pd.Series,
    trades: pd.DataFrame,  # 添加 trades 参数
    strategy_name: str,
    output_dir: str = "backtest_reports",
) -> Optional[str]:
    """
    使用 quantstats 生成 HTML 格式的回测报告。

    :param equity_curve: 包含总权益的 Pandas Series。
    :param trades: 包含交易记录的 Pandas DataFrame (用于 quantstats 的交易分析)。
    :param strategy_name: 策略名称，用于报告标题和文件名。
    :param output_dir: HTML报告的输出目录。
    :return: 生成的 HTML 文件路径，如果失败则返回 None。
    """
    if equity_curve.empty:
        logger.error("权益曲线为空，无法生成报告。")
        return None

    returns = equity_curve.pct_change().fillna(0)
    if returns.empty or (returns == 0).all() or len(returns) < 2:
        logger.warning("收益率序列为空、全为零或数据点过少，无法生成 quantstats 报告。")
        return None

    # 准备 trades 数据给 quantstats (需要特定列名) - 这部分仍然是可选的，且需要适配
    # QuantStats 的 reports.html 对 trades 的要求比较复杂，如果需要详细交易图表，
    # 可能需要构造一个包含如 'EntryTime', 'ExitTime', 'EntryPrice', 'ExitPrice', 'Contracts', 'Side' 等列的 DataFrame。
    # 目前我们主要依赖 returns 生成报告。
    qs_trades = None  # 暂时不传递 trades 给 qs.reports.html
    if not trades.empty:
        logger.info(
            "将使用收益率序列生成 QuantStats 报告。交易细节图表可能不包含在内，除非适配 trades 数据格式。"
        )
        # 如果未来要适配 trades，需要在这里转换 trades DataFrame 的格式

    try:
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)

        # 使用更独特的文件名避免冲突
        timestamp_str = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # 加毫秒
        report_filename = os.path.join(
            output_dir, f"{strategy_name}_回测报告_{timestamp_str}.html"
        )

        logger.info(f"准备生成 QuantStats HTML 报告: {report_filename}")
        # 生成报告 (可以尝试传入 qs_trades 如果你适配了格式)
        qs.reports.html(
            returns,
            # trades=qs_trades, # 如果适配了 trades，取消此行注释
            title=f"{strategy_name} 回测报告",
            output=report_filename,
            download_filename=report_filename,
        )

        logger.info(f"回测报告已生成: {report_filename}")
        return report_filename
    except Exception as e:
        logger.error(f"生成 QuantStats 回测报告时出错: {e}", exc_info=True)
        return None


def print_metrics(metrics: Dict[str, Optional[Any]]):
    """
    在控制台打印格式化的回测指标。

    :param metrics: 包含性能指标的字典。
    """
    print("\n--- 回测性能指标 ---")
    if not metrics:
        print("无指标可打印。")
        return
    if "错误" in metrics:
        print(f"错误: {metrics['错误']}")
        return

    # 定义打印顺序，使用新的或调整后的指标名称
    order = [
        "初始资金",
        "最终权益",
        "总回报 (%)",
        "年化回报 (%)",
        "最大回撤 (%)",
        "年化波动率 (%)",
        "夏普比率 (年化)",
        "索提诺比率 (年化)",
        "卡玛比率",
        "完整交易次数",  # 使用新的名称
        "盈利次数",
        "亏损次数",
        "胜率 (%)",
        "盈亏比",
        "平均盈利 ($)",
        "平均亏损 ($)",  # 这个值预期是负数或0
        "总亏损额 ($)",
        "总净盈利 ($)",  # 使用新的名称
        "总操作次数",  # 可以选择性显示
    ]

    printed_keys = set()
    for key in order:
        if key in metrics:
            value = metrics[key]
            # 格式化输出
            if value is None:
                print(f"{key:<20}: N/A")
            elif isinstance(value, (float, int)):
                if " (%)" in key:
                    print(f"{key:<20}: {value:.2f}%")
                elif " ($)" in key or "资金" in key or "权益" in key:
                    # 对平均亏损和总盈利使用特定格式
                    if key == "平均亏损 ($)":
                        print(f"{key:<20}: ${value:,.2f}")  # 直接显示负数
                    else:
                        print(f"{key:<20}: ${value:,.2f}")
                elif "次数" in key:
                    print(f"{key:<20}: {int(value)}")
                else:  # 其他比率
                    # 对盈亏比单独处理无穷大的情况
                    if key == "盈亏比" and value == float("inf"):
                        print(f"{key:<20}: inf")
                    elif key == "盈亏比":
                        print(f"{key:<20}: {value:.4f}")
                    else:  # 其他比率如夏普等
                        print(f"{key:<20}: {value:.4f}")

            else:  # 其他非数字类型直接打印
                print(f"{key:<20}: {value}")
            printed_keys.add(key)

    # 打印剩余未按顺序定义的指标
    for key, value in metrics.items():
        if key not in printed_keys and key != "错误":
            if value is None:
                print(f"{key:<20}: N/A")
            elif isinstance(value, (float, int)):
                # 简单格式化，可以根据需要调整
                print(f"{key:<20}: {value:.4f}")
            else:
                print(f"{key:<20}: {value}")

    print("--------------------\n")
