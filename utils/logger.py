# utils/logger.py
from pathlib import Path
import logging
from logging.handlers import TimedRotatingFileHandler

# 项目根目录
BASE_DIR = Path(__file__).resolve().parent.parent

# 日志目录
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / "temu.log"


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    # 避免重复添加 handler（非常关键）
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # ---------- 文件日志：每天一个 ----------
    file_handler = TimedRotatingFileHandler(
        filename=str(LOG_FILE),
        when="midnight",      # 每天 00:00 切割
        interval=1,
        backupCount=30,       # 保留 30 天
        encoding="utf-8",
        utc=False             # 用本地时间
    )
    file_handler.suffix = "%Y-%m-%d"  # 文件名后缀
    file_handler.setFormatter(formatter)

    # ---------- 控制台日志 ----------
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
