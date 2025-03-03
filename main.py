from utils import setup_logger
from datetime import datetime

from config import *
from utils import setup_logger, cache_data, load_cached_data
from models.pool import RaydiumPool
from models.transaction import RaydiumTransactionProcessor
from models.candle import CandleBuilder
from services.alchemy_service import AlchemyService
from services.raydium_service import RaydiumService
from services.data_processor import DataProcessor
import mplfinance as mpf
import pandas as pd

def main():
    # Настройка логирования
    logger = setup_logger()

    # Инициализация сервисов
    alchemy_service = AlchemyService(ALCHEMY_RPC_URL)
    raydium_service = RaydiumService()

    # Поиск пула
    pools = raydium_service.find_pools(MEME_TOKEN_MINT)
    active_pools = [pool for pool in pools if pool.is_active(ACTIVE_POOL_THRESHOLD.days)]
    if not active_pools:
        logger.error("Активные пулы не найдены")
        return

    pool = active_pools[0]
    logger.info(f"Используется пул: {pool.pool_address}")

    # Получение времени создания токена
    creation_time = alchemy_service.get_token_creation_time(MEME_TOKEN_MINT)
    if not creation_time:
        logger.error("Не удалось определить время создания токена")
        return
    logger.info(f"Время создания токена: {datetime.fromtimestamp(creation_time)} UTC")

    # Сбор и обработка данных
    processor = DataProcessor(pool, RaydiumTransactionProcessor(pool.base_mint, pool.quote_mint))
    prices = processor.process_transactions(creation_time)

    if not prices:
        logger.error("Цены не обнаружены")
        return

    # Построение свечей
    candles = CandleBuilder.build_candles(prices, CANDLE_INTERVAL)
    if not candles:
        logger.error("Не удалось построить свечи")
        return

    # Преобразование свечей в DataFrame для визуализации
    df = pd.DataFrame([candle.to_dict() for candle in candles])
    df.set_index("open_time", inplace=True)

    # Визуализация
    mpf.plot(
        df,
        type="candle",
        volume=True,
        style="yahoo",
        title=f"{MEME_TOKEN_MINT} первые {TARGET_CANDLES} минутных свечей",
        ylabel="Цена",
        ylabel_lower="Объем"
    )

    # Сохранение данных в CSV
    df.to_csv("candles.csv")
    logger.info("Данные сохранены в candles.csv")


if __name__ == "__main__":
    main()