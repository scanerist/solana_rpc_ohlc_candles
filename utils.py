import pickle
from typing import Any


import logging

def setup_logger():
    logger = logging.getLogger("SolanaOHLCLogger")  # Создаем логгер с именем
    logger.setLevel(logging.INFO)

    # Удаляем существующие обработчики, чтобы избежать дублирования
    if not logger.handlers:
        file_handler = logging.FileHandler("app.log", encoding="utf-8")
        stream_handler = logging.StreamHandler()

        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger
def cache_data(data: Any, filename: str):
    with open(filename, "wb") as f:
        pickle.dump(data, f)

def load_cached_data(filename: str) -> Any:
    try:
        with open(filename, "rb") as f:
            return pickle.load(f)
    except FileNotFoundError:
        return None