一个基于Python的合约自动交易  
可以通过在strategies文件夹下新建新策略并且通过参数调用进行使用  
代码很一坨,主要为策略backtest使用,实际交易函数疑似还有问题  
需要先在config/config.yaml修改API_KEY和SECRET_KEY  

以下readme由AI根据box_cli.py生成  
可能有错误,请发请求让我修

## 快速开始

### 1. 安装依赖

请确保已安装 Python 3.8+ 及相关依赖库：

```bash
pip install -r requirements.txt
```

### 2. 运行命令行工具

```bash
python box_cli.py --exchange binance --market_type future --api <你的API_KEY> --secret <你的SECRET_KEY> --strategy MyStrategy --symbols BTCUSDT,ETHUSDT
```

### 3. 参数说明

| 参数名                  | 类型    | 是否必填 | 默认值                | 说明                         |
|------------------------|--------|---------|----------------------|------------------------------|
| --exchange             | str    | 否      | binance              | 交易所名称                   |
| --market_type          | str    | 否      | future               | 市场类型（spot 或 future）   |
| --testnet              | flag   | 否      | False                | 是否使用测试网络             |
| --api                  | str    | 是      | 无                   | API Key                      |
| --secret               | str    | 是      | 无                   | Secret Key                   |
| --task_name            | str    | 否      | 随机UUID             | 任务名称                     |
| --strategy             | str    | 是      | 无                   | 策略类名称（如 MyStrategy）  |
| --symbols              | str    | 是      | 无                   | 交易对列表，逗号分隔         |
| --signal_interval      | float  | 否      | 1.0                  | 信号生成间隔（秒）           |
| --price_check_interval | float  | 否      | 0.2                  | 主循环检查间隔（秒）         |
| --leverage             | int    | 否      | 10                   | 合约杠杆倍数                 |
| --kline_limit          | int    | 否      | 1000                 | K线数量                      |
| --kline_interval_minutes | int  | 否      | 1                    | K线周期（分钟）              |
| --position_size        | float  | 否      | 0.1                  | 开仓保证金比例               |

### 4. 示例

```bash
python box_cli.py --exchange binance --market_type future --api abc123 --secret xyz456 --strategy MyStrategy --symbols BTCUSDT --leverage 20 --testnet
```

### 5. 常见问题

- **策略类如何自定义？**  
  请在 `strategies/` 目录下新建你的策略类，并确保类名与 `--strategy` 参数一致。

- **数据库文件在哪里？**  
  默认在当前目录下生成 `trading.db`。

- **如何中断程序？**  
  按 `Ctrl+C` 即可安全退出。
