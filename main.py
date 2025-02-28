import asyncio
import json

import aiohttp
import pandas as pd
from datetime import datetime, timedelta
import logging
import requests
import mplfinance as mpf

ALCHEMY_RPC_URL = "https://solana-mainnet.g.alchemy.com/v2/Kn9htzQrCRZ_qXj9OsxAhx1_xdbaOzyR"
MEME_TOKEN_MINT = "3GFFpfN7w9ZRPCFHKDH73NTLSV9jKyJF7HdYcjDzpump"
CANDLE_INTERVAL = 60
TARGET_CANDLES = 20
MAX_TRANSACTIONS = 5000


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def find_raydium_pools(token_mint: str) -> list:
    try:
        logging.info(f"Поиск пулов Raydium для {token_mint}")
        response = requests.get(
            "https://api.raydium.io/v2/sdk/liquidity/mainnet.json",
            timeout=30
        ).json()

        pools = []
        # Проверяем все категории пулов
        for pool_category in ["official", "unOfficial", "other"]:
            for pool in response.get(pool_category, []):
                if (
                    isinstance(pool, dict) and
                    "baseMint" in pool and
                    "quoteMint" in pool and
                    token_mint in (pool["baseMint"], pool["quoteMint"])
                ):
                    pools.append({
                        "pool_address": pool["id"],
                        "base_mint": pool["baseMint"],
                        "quote_mint": pool["quoteMint"]
                    })

        logging.info(f"Найдено {len(pools)} пулов")
        return pools

    except requests.exceptions.RequestException as e:
        logging.error(f"Сетевая ошибка: {e}")
        return []
    except ValueError as e:
        logging.error(f"Ошибка декодирования JSON: {e}")
        return []
    except Exception as e:
        logging.error(f"Неизвестная ошибка: {e}")
        return []

def get_pool_info():
    pools = find_raydium_pools(MEME_TOKEN_MINT)
    if not pools:
        logging.error(f"Пул для {MEME_TOKEN_MINT} не найден в Raydium")
        return None

    max_liquidity = 0
    selected_pool = None
    for pool in pools:
        try:
            info = requests.get(f"https://api.raydium.io/v2/pairs/{pool['pool_address']}").json()
            liquidity = int(info.get("liquidity", 0))
            if liquidity > max_liquidity:
                max_liquidity = liquidity
                selected_pool = pool
        except:
            continue

    if not selected_pool:
        selected_pool = pools[0]

    logging.info(f"Выбран пул: {selected_pool['pool_address']}")
    return {
        "pool": selected_pool["pool_address"],
        "base_mint": selected_pool["base_mint"],
        "quote_mint": selected_pool["quote_mint"]
    }


def get_signatures(pool_address):
    all_sigs = []
    two_days_ago = int((datetime.now() - timedelta(days=2)).timestamp())

    while True:
        response = requests.post(ALCHEMY_RPC_URL, json={
            "jsonrpc": "2.0",
            "method": "getSignaturesForAddress",
            "params": [
                pool_address,
                {"limit": 1000, "before": all_sigs[-1]["signature"] if all_sigs else None}
            ],
            "id": 1
        }).json()

        sigs = [
            s for s in response.get("result", [])
            if s.get("blockTime", 0) >= two_days_ago and s.get("err") is None
        ]

        if not sigs:
            break
        all_sigs.extend(sigs)
        if len(all_sigs) >= MAX_TRANSACTIONS:
            return all_sigs[:MAX_TRANSACTIONS][::-1]
    return all_sigs[::-1]


async def fetch_tx(session, signature):
    async with session.post(ALCHEMY_RPC_URL, json={
        "jsonrpc": "2.0",
        "method": "getTransaction",
        "params": [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
        "id": 1
    }) as resp:
        return await resp.json() if resp.status == 200 else None


def is_swap(tx):
    if not tx or "result" not in tx:
        return False
    return "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" in str(tx) and \
        len(tx["result"]["meta"].get("preTokenBalances", [])) >= 2


def extract_price(tx, base_mint):
    try:
        meta = tx["result"]["meta"]
        pre = meta["preTokenBalances"]
        post = meta["postTokenBalances"]

        base_change = 0
        quote_change = 0

        for p, pst in zip(pre, post):
            if p["mint"] == base_mint:
                base_change = abs(
                    float(pst["uiTokenAmount"]["uiAmountString"]) -
                    float(p["uiTokenAmount"]["uiAmountString"])
                )
            else:
                quote_change = abs(
                    float(pst["uiTokenAmount"]["uiAmountString"]) -
                    float(p["uiTokenAmount"]["uiAmountString"])
                )

        return quote_change / base_change if base_change and quote_change else None
    except Exception as e:
        logging.debug(f"Ошибка извлечения цены: {e}")
        return None


def create_candles(prices):
    candles = []
    current = None

    for ts in sorted(prices.keys()):
        dt = datetime.utcfromtimestamp(ts).replace(second=0, microsecond=0)
        price = prices[ts]

        if not current:
            current = {
                "open_time": dt,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 1
            }
            candles.append(current)
            continue

        while dt > current["open_time"] + timedelta(minutes=1):
            empty_time = current["open_time"] + timedelta(minutes=1)
            candles.append({
                "open_time": empty_time,
                "open": current["close"],
                "high": current["close"],
                "low": current["close"],
                "close": current["close"],
                "volume": 0
            })
            current = candles[-1]

        if dt == current["open_time"]:
            current["high"] = max(current["high"], price)
            current["low"] = min(current["low"], price)
            current["close"] = price
            current["volume"] += 1
        else:
            current = {
                "open_time": dt,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 1
            }
            candles.append(current)

        if len(candles) >= TARGET_CANDLES:
            break

    return candles[:TARGET_CANDLES]


async def main():
    pool_data = get_pool_info()
    if not pool_data:
        return

    RAYDIUM_POOL = pool_data["pool"]
    BASE_MINT = pool_data["base_mint"]

    signatures = get_signatures(RAYDIUM_POOL)
    if not signatures:
        logging.error("Транзакции не найдены")
        return

    async with aiohttp.ClientSession() as session:
        tx_details = await asyncio.gather(*[fetch_tx(session, s["signature"]) for s in signatures])

    prices = {}
    for tx in tx_details:
        if tx and is_swap(tx):
            price = extract_price(tx, BASE_MINT)
            if price:
                ts = tx["result"]["blockTime"]
                prices[ts] = price

    if not prices:
        logging.error("Цены не обнаружены")
        return

    candles = create_candles(prices)
    df = pd.DataFrame(candles)
    df["open_time"] = pd.to_datetime(df["open_time"])
    df.set_index("open_time", inplace=True)

    mpf.plot(df, type="candle", volume=True, style="yahoo")
    df.to_csv("candles.csv")


if __name__ == "__main__":
    asyncio.run(main())