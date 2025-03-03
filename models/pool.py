from utils import setup_logger
from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from datetime import datetime, timedelta

import requests

from config import ALCHEMY_RPC_URL

logger = setup_logger()


class BasePool(ABC):
    @abstractmethod
    def get_signatures(self, start_time: int) -> List[Dict]:
        """
        Получает подписи транзакций для пула начиная с указанного времени.
        """
        pass

    @abstractmethod
    def is_active(self, threshold_days: int) -> bool:
        """
        Проверяет, является ли пул активным за последние `threshold_days` дней.
        """
        pass


class RaydiumPool(BasePool):
    def __init__(self, pool_address: str, base_mint: str, quote_mint: str):
        self.pool_address = pool_address
        self.base_mint = base_mint
        self.quote_mint = quote_mint

    def get_signatures(self, start_time: int) -> List[Dict]:
        """
        Получает подписи транзакций для пула через Alchemy RPC.
        """
        try:
            response = requests.post(ALCHEMY_RPC_URL, json={
                "jsonrpc": "2.0",
                "method": "getSignaturesForAddress",
                "params": [
                    self.pool_address,
                    {"limit": 1000, "before": None, "until": start_time}
                ],
                "id": 1
            }).json()

            if "result" in response:
                return response["result"]
            return []

        except Exception as e:
            logger.error(f"Ошибка при получении подписей транзакций: {e}")
            return []

    def is_active(self, threshold_days: int) -> bool:
        """
        Проверяет активность пула за последние `threshold_days` дней.
        """
        try:
            threshold_time = int((datetime.now() - timedelta(days=threshold_days)).timestamp())
            signatures = self.get_signatures(threshold_time)
            return len(signatures) > 0
        except Exception as e:
            logger.error(f"Ошибка проверки активности пула: {e}")
            return False