import os
import subprocess

def kill_chrome_processes_alternative():
    """Альтернативный способ завершения процессов через psutil"""
    try:
        import psutil
        killed_count = 0

        for proc in psutil.process_iter(['pid', 'name']):
            try:
                if proc.info['name'] and 'chrome' in proc.info['name'].lower():
                    proc.kill()
                    killed_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass

        if killed_count > 0:
            print(f"   ✅ Завершено {killed_count} процессов chrome (psutil)")
        else:
            print(f"   ℹ️ Процессов chrome не найдено")

    except ImportError:
        print("   ⚠️ psutil не установлен, пропускаем")
    except Exception as e:
        print(f"   ❌ Ошибка при завершении процессов: {e}")