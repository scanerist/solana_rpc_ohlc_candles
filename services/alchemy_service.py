from utils import setup_logger

import requests
from typing import Dict, Optional
from datetime import datetime

logger = setup_logger()

class AlchemyService:
    def __init__(self, rpc_url: str):
        self.rpc_url = rpc_url

    def get_token_creation_time(self, token_mint: str) -> Optional[int]:
        """Определяет временную метку создания токена."""
        try:
            response = requests.post(self.rpc_url, json={
                "jsonrpc": "2.0",
                "method": "getSignaturesForAddress",
                "params": [token_mint, {"limit": 1}],
                "id": 1
            }).json()

            if "result" in response and len(response["result"]) > 0:
                oldest_signature = response["result"][0]
                block_time = oldest_signature.get("blockTime")
                if block_time:
                    return block_time
        except Exception as e:
            logger.error(f"Ошибка при получении времени создания токена: {e}")
        return None

    def get_transaction_details(self, signature: str) -> Optional[Dict]:
        """Получает детали транзакции."""
        try:
            response = requests.post(self.rpc_url, json={
                "jsonrpc": "2.0",
                "method": "getTransaction",
                "params": [signature, {"encoding": "jsonParsed"}],
                "id": 1
            }).json()

            return response.get("result")
        except Exception as e:
            logger.error(f"Ошибка получения транзакции: {e}")
            return None