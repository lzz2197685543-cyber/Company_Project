import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime


class SimpleLogger:
    """简单易用的日志配置类"""

    def __init__(self,name: str = 'app', log_dir: str = 'logs',level: str = 'INFO',max_size_mb: int = 10,backup_count: int = 5):
        """
        初始化日志配置

        Args:
            name: logger名称，同时也会作为日志文件名的一部分
            log_dir: 日志目录
            level: 日志级别 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            max_size_mb: 单个日志文件最大大小(MB)
            backup_count: 备份文件数量
        """
        # 确保日志目录存在
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # 获取当前年月日，格式为 YYYYMMDD
        current_date = datetime.now().strftime("%Y%m%d")

        # 完整的日志文件名：name_日期.log
        # 例如：myapp_20231208.log
        log_filename = f"{name}_{current_date}.log"

        # 完整的日志文件路径
        log_path = os.path.join(log_dir, log_filename)

        # 创建logger（使用传入的name）
        self.logger = logging.getLogger(name)

        # 设置日志级别
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        log_level = level_map.get(level.upper(), logging.INFO)
        self.logger.setLevel(log_level)

        # 清除现有的处理器，避免重复
        self.logger.handlers.clear()

        # 创建格式化器
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)

        # 创建文件处理器（支持日志轮转）
        file_handler = RotatingFileHandler(
            filename=log_path,
            maxBytes=max_size_mb * 1024 * 1024,  # 转换为字节
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)

        # 添加处理器到logger
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

        # 保存日志文件路径
        self.log_path = log_path

    # 提供方便的日志方法
    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.logger.critical(msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        self.logger.exception(msg, *args, **kwargs)

    def get_log_path(self):
        """获取当前日志文件路径"""
        return self.log_path


# 使用示例
if __name__ == "__main__":
    # 最简单的使用方式
    logger = SimpleLogger(name='myapp')
    logger.info("程序启动")
    logger.debug("调试信息")
    logger.error("发生错误")

    # 打印日志文件路径
    print(f"日志文件路径: {logger.get_log_path()}")
    # 输出类似：logs/myapp_20231208.log


    # 使用自定义配置
    # custom_logger = SimpleLogger(
    #     name='user_service',  # 这会生成 user_service_20231208.log
    #     log_dir='my_logs',
    #     level='DEBUG',
    #     max_size_mb=5,
    #     backup_count=3
    # )
    #
    # custom_logger.debug("自定义配置的调试日志")
    # custom_logger.info("用户 %s 登录成功", "张三")
    # print(f"自定义日志文件路径: {custom_logger.get_log_path()}")

