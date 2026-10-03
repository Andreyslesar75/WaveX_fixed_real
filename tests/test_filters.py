# tests/test_filters.py
"""Тесты filters.py: парсинг exchangeInfo, retry-инициализация, точечный refresh."""
from typing import Any, Mapping

import pytest

from trading.filters import FiltersCache, FiltersError, parse_exchange_info, parse_symbol_filters


def _symbol_entry(
    symbol: str = "RLCUSDT", status: str = "TRADING", market_lot: bool = True,
) -> dict[str, Any]:
    filters: list[dict[str, Any]] = [
        {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
    ]
    if market_lot:
        filters.append({"filterType": "MARKET_LOT_SIZE", "stepSize": "0.1", "minQty": "0.1", "maxQty": "10000"})
    else:
        filters.append({"filterType": "LOT_SIZE", "stepSize": "0.2", "minQty": "0.2", "maxQty": "5000"})
    filters += [
        {"filterType": "MIN_NOTIONAL", "notional": "5"},
        {"filterType": "PRICE_PROTECT", "triggerProtect": "0.1"},
        {"filterType": "FUTURE_SOME_NEW_FILTER", "whatever": 1},
    ]
    return {
        "symbol": symbol, "status": status, "pricePrecision": 4,
        "quantityPrecision": 1, "filters": filters,
    }


def _payload(*entries: Mapping[str, Any]) -> dict[str, Any]:
    return {"symbols": list(entries)}


class TestParsing:
    def test_full_parse(self) -> None:
        sf = parse_symbol_filters(_symbol_entry())
        assert sf.symbol == "RLCUSDT" and sf.is_trading
        assert str(sf.tick_size) == "0.0001"
        assert str(sf.step_size) == "0.1"
        assert str(sf.min_notional) == "5"
        assert str(sf.trigger_protect) == "0.1"

    def test_lot_size_fallback(self) -> None:
        sf = parse_symbol_filters(_symbol_entry(market_lot=False))
        assert str(sf.step_size) == "0.2"

    def test_min_notional_alt_field(self) -> None:
        entry = _symbol_entry()
        entry["filters"] = [
            f for f in entry["filters"] if f["filterType"] != "MIN_NOTIONAL"
        ] + [{"filterType": "MIN_NOTIONAL", "minNotional": "5"}]
        assert str(parse_symbol_filters(entry).min_notional) == "5"

    def test_missing_lot_filter_raises(self) -> None:
        entry = _symbol_entry()
        entry["filters"] = [
            f for f in entry["filters"] if "LOT_SIZE" not in f["filterType"]
        ]
        with pytest.raises(FiltersError):
            parse_symbol_filters(entry)

    def test_broken_payload_raises(self) -> None:
        with pytest.raises(FiltersError):
            parse_exchange_info({"foo": 1})

    def test_skip_broken_symbol(self) -> None:
        snap = parse_exchange_info(_payload(_symbol_entry(), _symbol_entry(symbol="")))
        assert "RLCUSDT" in snap and len(snap) == 1


class TestFiltersCache:
    async def test_init_retry_then_ok(self) -> None:
        calls: list[int] = []

        async def fetch(path: str, params: Mapping[str, str] | None) -> Any:
            calls.append(1)
            if len(calls) < 3:
                raise OSError("сеть недоступна")
            return _payload(_symbol_entry())

        cache = FiltersCache(fetch)
        await cache.initialize(attempts=5, backoff_start_s=0.0)
        assert cache.get("RLCUSDT") is not None
        assert cache.symbols_count == 1

    async def test_init_exhausted_raises(self) -> None:
        async def fetch(path: str, params: Mapping[str, str] | None) -> Any:
            raise OSError("нет сети")

        cache = FiltersCache(fetch)
        with pytest.raises(FiltersError):
            await cache.initialize(attempts=2, backoff_start_s=0.0)

    async def test_refresh_symbol(self) -> None:
        async def fetch(path: str, params: Mapping[str, str] | None) -> Any:
            return _payload(_symbol_entry())

        cache = FiltersCache(fetch)
        await cache.initialize(attempts=1, backoff_start_s=0.0)
        assert await cache.refresh_symbol("RLCUSDT") is True
        assert cache.get("NOPE") is None