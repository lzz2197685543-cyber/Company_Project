# util/logger.py
from pathlib import Path
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timedelta
import re

# 项目根目录
BASE_DIR = Path(__file__).resolve().parent.parent

# 日志目录
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)


def cleanup_old_logs(log_dir: Path, name: str, keep_days: int = 30):
    """
    删除超过指定天数的日志文件
    :param log_dir: 日志目录
    :param name: logger名称（用于匹配文件名）
    :param keep_days: 保留天数，默认30天
    """
    cutoff_date = datetime.now() - timedelta(days=keep_days)

    # 匹配日志文件名的模式，例如：my_logger-2024-01-15.log
    pattern = re.compile(rf"{re.escape(name)}-(\d{{4}}-\d{{2}}-\d{{2}})\.log")

    for log_file in log_dir.glob(f"{name}-*.log"):
        match = pattern.match(log_file.name)
        if match:
            file_date_str = match.group(1)
            try:
                file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
                if file_date < cutoff_date:
                    log_file.unlink()  # 删除文件
                    print(f"已删除旧日志文件: {log_file.name}")
            except ValueError:
                # 如果日期格式解析失败，跳过该文件
                continue


def get_logger(name: str, keep_days: int = 30) -> logging.Logger:
    """
    获取logger实例
    :param name: logger名称
    :param keep_days: 日志保留天数，默认30天
    """
    logger = logging.getLogger(name)

    # ⚠️ 避免重复添加 handler
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # ---------- 动态日志文件名 ----------
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = LOG_DIR / f"{name}-{today}.log"

    # ---------- 文件日志 ----------
    file_handler = TimedRotatingFileHandler(
        filename=str(log_file),
        when="midnight",
        interval=1,
        backupCount=keep_days,  # 使用keep_days作为backupCount
        encoding="utf-8",
        utc=False
    )
    file_handler.setFormatter(formatter)

    # ---------- 控制台日志 ----------
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # ---------- 清理旧日志 ----------
    cleanup_old_logs(LOG_DIR, name, keep_days)

    return logger