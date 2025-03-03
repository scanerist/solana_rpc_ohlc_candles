import asyncio
import json
import aiohttp
import pandas as pd
from datetime import datetime, timedelta, timezone
import logging
import requests
import mplfinance as mpf

# Конфигурация
ALCHEMY_RPC_URL = "https://solana-mainnet.g.alchemy.com/v2/Kn9htzQrCRZ_qXj9OsxAhx1_xdbaOzyR"
MEME_TOKEN_MINT = "3GFFpfN7w9ZRPCFHKDH73NTLSV9jKyJF7HdYcjDzpump"
CANDLE_INTERVAL = 60  # 1 минута
TARGET_CANDLES = 20
MAX_TRANSACTIONS = 10000
RAYDIUM_PROGRAM_ID = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def get_token_creation_time(token_mint: str) -> int:
    """
    Определяет временну́ю метку создания токена через его первые транзакции.
    Использует getSignaturesForAddress для поиска самой ранней транзакции.
    """
    try:
        # Получаем первую транзакцию для адреса токена
        response = requests.post(ALCHEMY_RPC_URL, json={
            "jsonrpc": "2.0",
            "method": "getSignaturesForAddress",
            "params": [token_mint, {"limit": 1}],
            "id": 1
        }).json()

        if "result" in response and len(response["result"]) > 0:
            oldest_signature = response["result"][0]
            block_time = oldest_signature.get("blockTime")
            if block_time:
                logging.info(f"Время создания токена: {datetime.fromtimestamp(block_time, tz=timezone.utc)} UTC")
                return block_time

    except Exception as e:
        logging.error(f"Ошибка при получении времени создания токена: {e}")

    # Fallback на неделю назад, если данные не найдены
    fallback_time = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp())
    logging.warning(f"Не удалось определить точное время создания токена. Используется fallback: {fallback_time}")
    return fallback_time


def find_raydium_pools(token_mint: str) -> list:
    """Находит пулы Raydium для указанного токена"""
    try:
        response = requests.get(
            "https://api.raydium.io/v2/sdk/liquidity/mainnet.json",
            timeout=30
        ).json()

        pools = []
        for pool_category in ["official", "unOfficial", "other"]:
            for pool in response.get(pool_category, []):
                if token_mint in (pool.get("baseMint"), pool.get("quoteMint")):
                    pools.append({
                        "pool_address": pool["id"],
                        "base_mint": pool["baseMint"],
                        "quote_mint": pool["quoteMint"]
                    })

        return pools

    except Exception as e:
        logging.error(f"Ошибка поиска пулов: {e}")
        return []


def get_signatures(pool_address: str, start_time: int) -> list:
    """Получает все подписи транзакций для пула начиная с указанного времени"""
    all_signatures = []
    before = None

    while True:
        response = requests.post(ALCHEMY_RPC_URL, json={
            "jsonrpc": "2.0",
            "method": "getSignaturesForAddress",
            "params": [
                pool_address,
                {"limit": 1000, "before": before}
            ],
            "id": 1
        }).json()

        if "result" not in response:
            break

        # Фильтруем и сортируем
        new_sigs = [
            sig for sig in response["result"]
            if sig.get("blockTime", 0) >= start_time and sig.get("err") is None
        ]

        if not new_sigs:
            break

        all_signatures.extend(new_sigs)
        before = new_sigs[-1]["signature"]

        if len(all_signatures) >= MAX_TRANSACTIONS:
            return all_signatures[:MAX_TRANSACTIONS][::-1]  # Реверс для хронологического порядка

    return all_signatures[::-1]  # Реверс для хронологического порядка


async def fetch_transaction(session: aiohttp.ClientSession, signature: str) -> dict:
    """Асинхронно получает детали транзакции"""
    try:
        async with session.post(ALCHEMY_RPC_URL, json={
            "jsonrpc": "2.0",
            "method": "getTransaction",
            "params": [signature, {"encoding": "jsonParsed"}],
            "id": 1
        }) as response:
            return await response.json()
    except Exception as e:
        logging.warning(f"Ошибка получения транзакции {signature}: {e}")
        return {}


def is_swap_transaction(tx: dict, base_mint: str, quote_mint: str) -> bool:
    """Проверяет, является ли транзакция свапом в пуле Raydium"""
    try:
        meta = tx.get("result", {}).get("meta", {})
        if not meta:
            return False

        # Проверка программы Raydium
        instructions = tx["result"]["transaction"]["message"]["instructions"]
        for instr in instructions:
            if instr.get("programId", "") == RAYDIUM_PROGRAM_ID:
                data = instr.get("data", "")
                if data.startswith("swap"):
                    return True

        return False

    except Exception as e:
        logging.debug(f"Ошибка проверки свапа: {e}")
        return False


def extract_swap_price(tx: dict, base_mint: str, quote_mint: str) -> float | None:
    """Извлекает цену из свап-транзакции"""
    try:
        meta = tx["result"]["meta"]
        pre_balances = meta["preTokenBalances"]
        post_balances = meta["postTokenBalances"]

        base_amount = 0.0
        quote_amount = 0.0

        for pre, post in zip(pre_balances, post_balances):
            if pre["mint"] == base_mint:
                delta = float(post["uiTokenAmount"]["uiAmountString"]) - float(pre["uiTokenAmount"]["uiAmountString"])
                base_amount += abs(delta)
            elif pre["mint"] == quote_mint:
                delta = float(post["uiTokenAmount"]["uiAmountString"]) - float(pre["uiTokenAmount"]["uiAmountString"])
                quote_amount += abs(delta)

        return quote_amount / base_amount if base_amount != 0 else None

    except Exception as e:
        logging.debug(f"Ошибка извлечения цены: {e}")
        return None


def build_candles(prices: dict[int, float]) -> pd.DataFrame:
    """Строит свечи из временных меток и цен"""
    sorted_times = sorted(prices.keys())
    start_time = datetime.fromtimestamp(sorted_times[0], tz=timezone.utc).replace(second=0, microsecond=0)
    candles = []

    for i in range(TARGET_CANDLES):
        candle_start = start_time + timedelta(minutes=i)
        candle_end = candle_start + timedelta(minutes=1)

        # Фильтруем цены в интервале
        candle_prices = [
            price for ts, price in prices.items()
            if candle_start.timestamp() <= ts < candle_end.timestamp()
        ]

        if not candle_prices:
            # Заполняем пустые свечи значениями предыдущей свечи
            candles.append({
                "open_time": candle_start,
                "open": candles[-1]["close"] if candles else None,
                "high": candles[-1]["close"] if candles else None,
                "low": candles[-1]["close"] if candles else None,
                "close": candles[-1]["close"] if candles else None,
                "volume": 0
            })
            continue

        candles.append({
            "open_time": candle_start,
            "open": candle_prices[0],
            "high": max(candle_prices),
            "low": min(candle_prices),
            "close": candle_prices[-1],
            "volume": sum(candle_prices)  # Суммируем квотируемый объем
        })

    return pd.DataFrame(candles).set_index("open_time")


async def main():
    # 1. Поиск пула Raydium
    pools = find_raydium_pools(MEME_TOKEN_MINT)
    if not pools:
        logging.error("Пулы Raydium не найдены")
        return

    # Выбираем пул с наибольшим количеством транзакций
    pool = next((p for p in pools if MEME_TOKEN_MINT in (p["base_mint"], p["quote_mint"])), None)
    if not pool:
        logging.error("Пул для указанного токена не найден")
        return

    base_mint = pool["base_mint"]
    quote_mint = pool["quote_mint"]
    logging.info(f"Используется пул: {pool['pool_address']}")

    # 2. Получение времени создания токена
    creation_time = get_token_creation_time(MEME_TOKEN_MINT)
    logging.info(f"Время создания токена: {datetime.fromtimestamp(creation_time, tz=timezone.utc)} UTC")

    # 3. Сбор транзакций
    signatures = get_signatures(pool["pool_address"], creation_time)
    if not signatures:
        logging.error("Транзакции не найдены")
        return

    # 4. Парсинг транзакций
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_transaction(session, sig["signature"]) for sig in signatures]
        transactions = await asyncio.gather(*tasks)

    # 5. Обработка свапов и извлечение цен
    prices = {}
    for tx in transactions:
        if is_swap_transaction(tx, base_mint, quote_mint):
            price = extract_swap_price(tx, base_mint, quote_mint)
            if price:
                block_time = tx["result"]["blockTime"]
                prices[block_time] = price
                logging.debug(f"Обнаружен свап: цена={price}, время={datetime.fromtimestamp(block_time, tz=timezone.utc)}")

    if not prices:
        logging.error("Цены не обнаружены")
        return

    # 6. Построение свечей
    df = build_candles(prices)
    if df.empty:
        logging.error("Не удалось построить свечи")
        return

    # 7. Визуализация
    mpf.plot(
        df,
        type="candle",
        volume=True,
        style="yahoo",
        title=f"{MEME_TOKEN_MINT} первые {TARGET_CANDLES} минутных свечей",
        ylabel="Цена",
        ylabel_lower="Объем"
    )

    # Сохранение в CSV
    df.to_csv("candles.csv")
    logging.info("Данные сохранены в candles.csv")


if __name__ == "__main__":
    asyncio.run(main())