from typing import Dict, List
from datetime import datetime, timedelta
from utils import setup_logger

logger = setup_logger()

class Candle:
    def __init__(self, open_time: datetime, open_price: float, high: float, low: float, close: float, volume: float):
        self.open_time = open_time
        self.open = open_price
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

    def to_dict(self) -> Dict:
        """
        Преобразует объект свечи в словарь для удобства работы.
        """
        return {
            "open_time": self.open_time,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume
        }


class CandleBuilder:
    @staticmethod
    def build_candles(prices: Dict[int, float], interval: int) -> List[Candle]:
        """
        Строит свечи из временных меток и цен.
        """
        sorted_times = sorted(prices.keys())
        start_time = datetime.fromtimestamp(sorted_times[0]).replace(second=0, microsecond=0)
        candles = []

        for i in range(len(sorted_times)):
            candle_start = start_time + timedelta(seconds=i * interval)
            candle_end = candle_start + timedelta(seconds=interval)

            candle_prices = [
                price for ts, price in prices.items()
                if candle_start.timestamp() <= ts < candle_end.timestamp()
            ]

            if not candle_prices:
                continue

            candles.append(Candle(
                open_time=candle_start,
                open=candle_prices[0],
                high=max(candle_prices),
                low=min(candle_prices),
                close=candle_prices[-1],
                volume=sum(candle_prices)
            ))

        return candles