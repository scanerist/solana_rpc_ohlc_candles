from utils import setup_logger
from abc import ABC, abstractmethod
from typing import Optional

logger = setup_logger()

class BaseTransactionProcessor(ABC):
    @abstractmethod
    def is_swap_transaction(self, tx: dict) -> bool:
        """
        Проверяет, является ли транзакция свапом.
        """
        pass

    @abstractmethod
    def extract_swap_price(self, tx: dict) -> Optional[float]:
        """
        Извлекает цену из свап-транзакции.
        """
        pass


class RaydiumTransactionProcessor(BaseTransactionProcessor):
    def __init__(self, base_mint: str, quote_mint: str):
        self.base_mint = base_mint
        self.quote_mint = quote_mint

    def is_swap_transaction(self, tx: dict) -> bool:
        """
        Проверяет, является ли транзакция свапом.
        """
        try:
            instructions = tx["result"]["transaction"]["message"]["instructions"]
            for instr in instructions:
                if instr.get("programId", "") == "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8":
                    data = instr.get("data", "")
                    if data.startswith("swap"):
                        return True
            return False
        except Exception as e:
            logger.debug(f"Ошибка проверки свапа: {e}")
            return False

    def extract_swap_price(self, tx: dict) -> Optional[float]:
        """
        Извлекает цену из свап-транзакции.
        """
        try:
            meta = tx["result"]["meta"]
            pre_balances = meta["preTokenBalances"]
            post_balances = meta["postTokenBalances"]

            base_amount = 0.0
            quote_amount = 0.0

            for pre, post in zip(pre_balances, post_balances):
                if pre["mint"] == self.base_mint:
                    delta = float(post["uiTokenAmount"]["uiAmountString"]) - float(pre["uiTokenAmount"]["uiAmountString"])
                    base_amount += abs(delta)
                elif pre["mint"] == self.quote_mint:
                    delta = float(post["uiTokenAmount"]["uiAmountString"]) - float(pre["uiTokenAmount"]["uiAmountString"])
                    quote_amount += abs(delta)

            return quote_amount / base_amount if base_amount != 0 else None
        except Exception as e:
            logger.debug(f"Ошибка извлечения цены: {e}")
            return None