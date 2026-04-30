import os
import signal
import subprocess

def kill_chrome_processes():
    """Закрывает все процессы Google Chrome"""
    try:
        if os.name == 'nt':  # Windows
            subprocess.run(['taskkill', '/F', '/IM', 'chrome.exe'],
                         capture_output=True, check=False)
        else:  # Linux/Mac
            subprocess.run(['pkill', '-f', 'chrome'],
                         capture_output=True, check=False)
    except Exception:
        pass


if __name__ == '__main__':
    kill_chrome_processes()
