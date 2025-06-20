import os
import yaml
from ..utils.logger import logger  # 使用相对导入或确保 PYTHONPATH 正确

# 假设 config.yaml 在 config 目录内
DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")


class ConfigManager:
    """
    配置管理器，负责加载和管理交易配置

    支持从环境变量和配置文件读取配置
    提供默认配置和自定义配置的融合
    """

    def __init__(self, config_path=DEFAULT_CONFIG_PATH):
        """
        初始化配置管理器

        :param config_path: 配置文件路径
        """
        self.config_path = config_path
        self.config = self._load_config()
        logger.info(f"配置文件已加载: {config_path}")

    def _load_config(self):
        """
        加载配置文件

        :return: 配置字典
        """
        # 默认配置 (可以根据需要添加更多默认值)
        default_config = {
            "logging": {
                "log_dir": "logs",
                "level": "INFO",
                "max_file_size": "10 MB",
                "retention_days": 7,
            },
            "database": {
                # 数据库路径现在由 box_cli.py 确定或使用默认值
                # 'path': 'trading.db'
            },
            "exchange": {
                "name": "binance",
                "market_type": "future",
                "binance": {  # 示例：为特定交易所存储 API 密钥（不推荐直接硬编码）
                    "api_key": os.environ.get("BINANCE_API_KEY"),
                    "secret_key": os.environ.get("BINANCE_SECRET_KEY"),
                },
                # 可以为其他交易所添加类似结构
                "xt": {
                    "api_key": os.environ.get("XT_API_KEY"),
                    "secret_key": os.environ.get("XT_SECRET_KEY"),
                },
            },
        }

        # 如果配置文件存在，则覆盖默认配置
        if os.path.exists(self.config_path):
            logger.info(f"找到配置文件: {self.config_path}")
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    user_config = yaml.safe_load(f)
                    if user_config:  # 确保文件不是空的
                        self._deep_merge(default_config, user_config)
                    else:
                        logger.warning(f"配置文件 {self.config_path} 为空")
            except Exception as e:
                logger.error(f"加载或解析配置文件 {self.config_path} 失败: {e}")
        else:
            logger.warning(
                f"配置文件未找到: {self.config_path}, 将使用默认配置和环境变量"
            )

        # 从环境变量加载敏感信息 (覆盖文件配置)
        for exch, keys in default_config.get("exchange", {}).items():
            if isinstance(keys, dict) and "api_key" in keys:
                env_api_key = os.environ.get(f"{exch.upper()}_API_KEY")
                env_secret_key = os.environ.get(
                    f"{exch.upper()}_SECRET"
                )  # 保持与 box_cli 一致
                if env_api_key:
                    default_config["exchange"][exch]["api_key"] = env_api_key
                    logger.debug(f"从环境变量加载 {exch.upper()}_API_KEY")
                if env_secret_key:
                    default_config["exchange"][exch]["secret_key"] = env_secret_key
                    logger.debug(f"从环境变量加载 {exch.upper()}_SECRET")

        return default_config

    def _deep_merge(self, base, update):
        """
        深度合并两个字典

        :param base: 基础字典
        :param update: 待合并的字典
        """
        for key, value in update.items():
            if isinstance(value, dict) and key in base and isinstance(base[key], dict):
                base[key] = self._deep_merge(base.get(key, {}), value)
            else:
                base[key] = value
        return base

    def get(self, key, default=None):
        """
        获取配置项

        :param key: 配置键，支持点分隔的多级键 (例如 'exchange.binance.api_key')
        :param default: 默认值
        :return: 配置值
        """
        keys = key.split(".")
        value = self.config
        try:
            for k in keys:
                if isinstance(value, dict):
                    value = value[k]  # 使用 [] 而不是 get，如果键不存在则会报错
                else:
                    # 如果路径中间不是字典，则无法继续查找
                    logger.warning(
                        f"配置路径 '{key}' 中的 '{k}' 不是字典，无法继续查找"
                    )
                    return default
            return value
        except KeyError:
            # logger.debug(f"在配置中未找到键 '{key}'，返回默认值 {default}")
            return default
        except Exception as e:
            logger.error(f"获取配置项 '{key}' 时出错: {e}")
            return default

    def update(self, key, value):
        """
        在内存中更新配置项（不会写回文件）

        :param key: 配置键，支持点分隔的多级键
        :param value: 配置值
        """
        keys = key.split(".")
        d = self.config
        for k in keys[:-1]:
            d = d.setdefault(k, {})  # 如果键不存在，创建空字典
            if not isinstance(d, dict):
                logger.error(f"无法更新配置 '{key}'，因为路径中的 '{k}' 不是字典")
                return
        if keys:
            d[keys[-1]] = value
            logger.debug(f"配置项 '{key}' 已在内存中更新")


# 全局配置管理器实例 (按需使用)
# config_manager = ConfigManager()
