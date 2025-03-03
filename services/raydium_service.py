from utils import setup_logger

import requests
from typing import List, Dict, Optional

from config import ALCHEMY_RPC_URL
from models.pool import RaydiumPool
from datetime import datetime, timedelta

logger = setup_logger()

class RaydiumService:
    def __init__(self):
        self.api_url = "https://api.raydium.io/v2/sdk/liquidity/mainnet.json"

    def find_pools(self, token_mint: str) -> List[RaydiumPool]:
        """
        Находит пулы Raydium для указанного токена.
        """
        try:
            response = requests.get(self.api_url, timeout=30).json()
            pools = []

            for pool_category in ["official", "unOfficial", "other"]:
                for pool_data in response.get(pool_category, []):
                    if token_mint in (pool_data.get("baseMint"), pool_data.get("quoteMint")):
                        pools.append(
                            RaydiumPool(
                                pool_address=pool_data["id"],
                                base_mint=pool_data["baseMint"],
                                quote_mint=pool_data["quoteMint"]
                            )
                        )

            return pools

        except Exception as e:
            logger.error(f"Ошибка при поиске пулов Raydium: {e}")
            return []

    def get_pool_activity(self, pool_address: str, start_time: int) -> List[Dict]:
        """
        Получает активность пула (подписи транзакций) начиная с указанного времени.
        """
        try:
            response = requests.post(ALCHEMY_RPC_URL, json={
                "jsonrpc": "2.0",
                "method": "getSignaturesForAddress",
                "params": [
                    pool_address,
                    {"limit": 1000, "before": None, "until": start_time}
                ],
                "id": 1
            }).json()

            if "result" in response:
                return response["result"]
            return []

        except Exception as e:
            logger.error(f"Ошибка при получении активности пула: {e}")
            return []