import asyncio
from utils import setup_logger

import aiohttp
from typing import Dict, Optional

from config import ALCHEMY_RPC_URL
from models.transaction import BaseTransactionProcessor
from models.pool import BasePool
from datetime import datetime
from tqdm import tqdm


logger = setup_logger()


class DataProcessor:
    def __init__(self, pool: BasePool, transaction_processor: BaseTransactionProcessor):
        self.pool = pool
        self.transaction_processor = transaction_processor

    async def fetch_transaction(self, session: aiohttp.ClientSession, signature: str) -> Optional[Dict]:
        """
        Асинхронно получает детали транзакции.
        """
        try:
            async with session.post(ALCHEMY_RPC_URL, json={
                "jsonrpc": "2.0",
                "method": "getTransaction",
                "params": [signature, {"encoding": "jsonParsed"}],
                "id": 1
            }) as response:
                data = await response.json()
                return data.get("result")
        except Exception as e:
            logger.warning(f"Ошибка получения транзакции {signature}: {e}")
            return None

    async def process_transactions(self, start_time: int) -> Dict[int, float]:
        """
        Собирает и обрабатывает транзакции для построения цен.
        """
        prices = {}
        signatures = self.pool.get_signatures(start_time)

        if not signatures:
            logger.error("Транзакции не найдены")
            return {}

        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_transaction(session, sig["signature"]) for sig in signatures]
            transactions = await asyncio.gather(*tasks)

        for tx in tqdm(transactions, desc="Обработка транзакций"):
            if tx and self.transaction_processor.is_swap_transaction(tx):
                price = self.transaction_processor.extract_swap_price(tx)
                if price:
                    block_time = tx["blockTime"]
                    prices[block_time] = price

        return prices