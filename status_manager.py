import json
import time
import logging
import os  # <-- ДОБАВЛЯЕМ ЭТОТ ИМПОРТ
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)

# Константы статусов
STATUS_IDLE = "idle"  # Ожидание
STATUS_RUNNING = "running"  # В работе
STATUS_STOPPED = "stopped"  # Выключен

STATUS_FILE = 'parser_status.txt'

# Человекочитаемые названия статусов
STATUS_NAMES = {
    STATUS_IDLE: "Ожидание",
    STATUS_RUNNING: "В работе",
    STATUS_STOPPED: "Выключен"
}

# CSS классы для статусов
STATUS_CLASSES = {
    STATUS_IDLE: "status-idle",
    STATUS_RUNNING: "status-running",
    STATUS_STOPPED: "status-stopped"
}


def save_status(status: str, message: str = "") -> None:
    """Сохраняет статус парсера в файл"""
    data = {
        'status': status,
        'message': message,
        'last_run': datetime.now().isoformat(),
        'last_run_time': time.time()
    }
    try:
        with open(STATUS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Статус сохранен: {status} - {message}")
    except Exception as e:
        logger.error(f"Ошибка сохранения статуса: {e}")


def load_status() -> Dict[str, Any]:
    """Загружает статус парсера из файла"""
    try:
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if 'last_run_time' not in data:
                    data['last_run_time'] = 0
                return data
    except Exception as e:
        logger.error(f"Ошибка загрузки статуса: {e}")
    return {'status': STATUS_IDLE, 'message': 'Готов к работе', 'last_run': None, 'last_run_time': 0}


def get_status_display() -> Dict[str, Any]:
    """Возвращает статус для отображения в веб-интерфейсе"""
    status_data = load_status()
    status = status_data.get('status', STATUS_IDLE)

    return {
        'status': status,
        'status_name': STATUS_NAMES.get(status, "Неизвестно"),
        'status_class': STATUS_CLASSES.get(status, ""),
        'message': status_data.get('message', ''),
        'last_run': status_data.get('last_run', ''),
        'last_run_time': status_data.get('last_run_time', 0)
    }


def is_running() -> bool:
    """Проверяет, выполняется ли скрипт"""
    status_data = load_status()
    return status_data.get('status') == STATUS_RUNNING


def is_stopped() -> bool:
    """Проверяет, выключен ли парсер"""
    status_data = load_status()
    return status_data.get('status') == STATUS_STOPPED


def can_auto_start() -> bool:
    """Проверяет, можно ли запускать автоцикл"""
    status_data = load_status()
    return status_data.get('status') != STATUS_STOPPED