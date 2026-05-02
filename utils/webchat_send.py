import pyautogui
import time
import pyperclip
import subprocess
import os
from utils.logger import get_logger


def open_wechat():
    """打开微信"""
    wechat_paths = [
        "C:\Program Files\Tencent\Weixin\\Weixin.exe",
    ]

    for path in wechat_paths:
        if os.path.exists(path):
            subprocess.Popen(path)
            time.sleep(3)  # 等待微信启动
            return True

    print("未找到微信，请手动打开微信窗口")
    return False

def send_wechat_message(contact_name, message):
    """发送微信消息"""
    # 搜索联系人
    pyautogui.hotkey('ctrl', 'f')
    time.sleep(0.5)

    pyperclip.copy(contact_name)
    time.sleep(0.2)
    pyautogui.hotkey('ctrl', 'v')
    time.sleep(1)

    # 进入聊天窗口
    pyautogui.press('enter')
    time.sleep(1)

    # 发送消息
    pyperclip.copy(message)
    pyautogui.hotkey('ctrl', 'v')
    time.sleep(0.3)
    pyautogui.press('enter')

def webchat_send(contacts):
    print("正在打开微信...")
    if not open_wechat():
        print("请手动打开微信窗口")
        return

    time.sleep(0.5)

    for name1, msg in contacts:
        print(f"正在发送给：{name1}")
        send_wechat_message(name1, msg)
        time.sleep(2)

    print("全部消息发送完成！")

    # 👉 发送 ESC 键
    pyautogui.press('esc')
    print("已按下 ESC 键")


if __name__ == "__main__":
    webchat_send()