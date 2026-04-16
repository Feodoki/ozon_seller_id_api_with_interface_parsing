import os
import subprocess


def kill_chrome_processes():
    """Принудительно завершает все процессы chrome.exe"""
    try:

        # Способ 1: через taskkill (Windows)
        if os.name == 'nt':  # Windows
            result = subprocess.run(['taskkill', '/F', '/IM', 'chrome.exe'],
                                    capture_output=True, text=True)

        # Способ 2: через pkill (Linux/Mac)
        else:
            result = subprocess.run(['pkill', '-f', 'chrome'],
                                    capture_output=True, text=True)

        # Дополнительная очистка: убиваем все дочерние процессы chrome
        if os.name == 'nt':
            subprocess.run(['taskkill', '/F', '/IM', 'chromedriver.exe'],
                           capture_output=True, text=True)

    except Exception as e:
        pass