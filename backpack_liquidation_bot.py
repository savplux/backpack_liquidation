# backpack_liquidation_bot.py – v12.6 (Fixed Syntax Error)
"""
Paired long/short liquidation bot for Backpack Exchange, v12.6.

Workflow:
1. Deposit `initial_deposit` USDC to each sub-account and log.
2. Random delay between `action_delay.min` and `action_delay.max` seconds.
3. Open market orders using all margin via BackpackTrader.
4. Monitor until one position liquidates.
5. Close the surviving position.
6. Sweep remaining USDC to main account and log.
7. Repeat indefinitely.
"""
from __future__ import annotations

import logging
import threading
import time
import sys
import random
from pathlib import Path
from typing import Dict, List

import yaml
from backpack_exchange_sdk.authenticated import AuthenticationClient

# Utility trader class for full-margin orders
from backpack_exchange_sdk.public import PublicClient
import math

class BackpackTrader:
    def __init__(self, api_key, api_secret):
        self.auth = AuthenticationClient(api_key, api_secret)
        self.pub = PublicClient()
        self._market_cache = {}
        
    def get_available_margin(self) -> float:
        """Return available USDC margin via collateral endpoint."""
        try:
            resp = self.auth._send_request(
                "GET", "api/v1/capital/collateral", "collateralQuery", {}
            )
            data = resp.get("data", resp) if isinstance(resp, dict) else resp
            items = data.get("collateral", data) if isinstance(data, dict) else data
            for itm in items:
                if itm.get("symbol") == "USDC":
                    return float(itm.get("availableQuantity", 0) or 0)
            return 0.0
        except Exception as e:
            logging.error(f"BackpackTrader | margin fetch error: {e}")
            return 0.0
    
    def get_ticker_price(self, symbol: str) -> float:
        """Get current market price from ticker endpoint."""
        try:
            # Сначала используем публичный API для получения цены
            ticker = self.pub.get_ticker(symbol)
            if isinstance(ticker, dict) and "lastPrice" in ticker:
                return float(ticker["lastPrice"])
            elif isinstance(ticker, dict) and "data" in ticker and "lastPrice" in ticker["data"]:
                return float(ticker["data"]["lastPrice"])
                
            # Если публичный API не сработал, попробуем аутентифицированный API
            resp = self.auth._send_request("GET", f"api/v1/ticker/{symbol}", "tickerQuery", {})
            if isinstance(resp, dict) and "lastPrice" in resp:
                return float(resp["lastPrice"])
            elif isinstance(resp, dict) and "data" in resp and "lastPrice" in resp["data"]:
                return float(resp["data"]["lastPrice"])
                
            # Получаем orderbook и используем среднюю цену между лучшими бидом и аском
            orderbook = self.pub.get_order_book(symbol)
            if isinstance(orderbook, dict):
                bids = orderbook.get("bids", [])
                asks = orderbook.get("asks", [])
                if bids and asks:
                    best_bid = float(bids[0][0])
                    best_ask = float(asks[0][0])
                    return (best_bid + best_ask) / 2
                    
            logging.warning(f"BackpackTrader | Failed to get ticker price for {symbol}, using backup method")
            # Получаем цену через поиск сделок
            trades = self.pub.get_trades(symbol, limit=1)
            if isinstance(trades, list) and trades and "price" in trades[0]:
                return float(trades[0]["price"])
            
            # Если всё ещё нет цены, возвращаем резервное значение
            return 138.0  # Примерная текущая цена SOL
        except Exception as e:
            logging.error(f"BackpackTrader | ticker price error: {e}")
            return 138.0  # Примерная текущая цена SOL
            
    def get_market_info(self, symbol: str) -> Dict:
        """Fetch market info with step size information."""
        try:
            # Используем кэш, если есть
            if symbol in self._market_cache:
                cached_info = self._market_cache[symbol]
                # Дополним кэшированные данные актуальной ценой
                if "lastPrice" not in cached_info or cached_info["lastPrice"] == "0":
                    price = self.get_ticker_price(symbol)
                    cached_info["lastPrice"] = str(price)
                return cached_info
                
            # Получаем базовую информацию о рынке
            resp = self.auth._send_request("GET", "api/v1/markets", "marketQuery", {})
            markets = []
            if isinstance(resp, dict) and "data" in resp:
                markets = resp["data"]
            elif isinstance(resp, list):
                markets = resp
                
            # Ищем нужный рынок
            market_info = None
            for m in markets:
                if m.get("symbol") == symbol:
                    market_info = m
                    break
                    
            if not market_info:
                # Прямой запрос к конкретному рынку
                resp = self.auth._send_request("GET", f"api/v1/markets/{symbol}", "marketQuery", {})
                if isinstance(resp, dict):
                    market_info = resp.get("data", resp)
                    
            if market_info:
                # Извлекаем stepSize из filters.quantity
                filters = market_info.get("filters", {})
                quantity_filter = filters.get("quantity", {})
                
                # Добавляем lastPrice из тикера
                if "lastPrice" not in market_info or market_info["lastPrice"] == "0":
                    price = self.get_ticker_price(symbol)
                    market_info["lastPrice"] = str(price)
                    
                # Добавляем или обновляем stepSize
                if "stepSize" not in quantity_filter:
                    quantity_filter["stepSize"] = "0.01"  # Резервное значение
                    
                market_info["baseIncrement"] = quantity_filter["stepSize"]
                self._market_cache[symbol] = market_info
                return market_info
                
            # Резервные данные, если ничего не нашли
            logging.warning(f"BackpackTrader | Using fallback market info for {symbol}")
            fallback = {
                "symbol": symbol, 
                "lastPrice": str(self.get_ticker_price(symbol)),
                "baseIncrement": "0.01"
            }
            self._market_cache[symbol] = fallback
            return fallback
            
        except Exception as e:
            logging.error(f"BackpackTrader | market info error: {e}")
            # Резервные значения в случае ошибки
            price = self.get_ticker_price(symbol)
            fallback = {
                "symbol": symbol, 
                "lastPrice": str(price),
                "baseIncrement": "0.01"
            }
            self._market_cache[symbol] = fallback
            return fallback
            
    def execute_full_margin_order(self, symbol: str, side: str, leverage: float = 1.0, retry_attempts: int = 5, min_delay: float = 1.0, max_delay: float = 15.0) -> bool:
        """Place a market order using all available margin with better error handling."""
        # Всего retry_attempts попыток
        for attempt in range(retry_attempts):
            try:
                # 1. Сначала пробуем метод с quoteQuantity (до 4 знаков после запятой)
                margin = self.get_available_margin() * leverage
                if margin <= 0:
                    logging.error(f"BackpackTrader | No margin available")
                    return False
                    
                # Округляем до 4 знаков после запятой, чтобы избежать ошибки "decimal too long"
                quote_qty = round(margin, 4)
                quote_qty_str = f"{quote_qty:.4f}"
                
                logging.info(f"BackpackTrader | Попытка {attempt+1}/{retry_attempts}: ордер на {quote_qty_str} USDC ({side})")
                result = self.auth.execute_order(
                    orderType="Market",
                    side=side,
                    symbol=symbol,
                    quoteQuantity=quote_qty_str,
                    reduceOnly=False,
                    autoBorrow=True,
                    autoBorrowRepay=True,
                    autoLend=True,
                    autoLendRedeem=True,
                    selfTradePrevention="RejectTaker"
                )
                logging.info(f"BackpackTrader | Placed {side} order for {symbol} with quoteQuantity={quote_qty_str}")
                return True
            except Exception as e:
                logging.warning(f"BackpackTrader | quoteQuantity order error (попытка {attempt+1}/{retry_attempts}): {e}")
                
                # 2. Если первый метод не сработал, пробуем вычислить quantity
                try:
                    info = self.get_market_info(symbol)
                    price = float(info.get("lastPrice", "0"))
                    step = float(info.get("baseIncrement", "0.01"))
                    
                    if price <= 0:
                        price = self.get_ticker_price(symbol)
                        
                    if price <= 0:
                        logging.error(f"BackpackTrader | Could not get valid price for {symbol}")
                        
                        # Если не последняя попытка, ждем и пробуем снова
                        if attempt < retry_attempts - 1:
                            delay = random.uniform(min_delay, max_delay)
                            logging.info(f"BackpackTrader | Ожидание {delay:.1f}с перед следующей попыткой")
                            time.sleep(delay)
                        continue  # Переходим к следующей попытке
                        
                    margin = self.get_available_margin() * leverage
                    if margin <= 0:
                        logging.error(f"BackpackTrader | No margin available")
                        return False
                        
                    # Вычисляем количество с учетом шага
                    raw_qty = margin / price
                    steps = math.floor(raw_qty / step)
                    qty = steps * step
                    
                    # Минимальное значение
                    if qty < 0.01:
                        qty = 0.01
                        
                    # Округляем до правильного количества знаков
                    decimals = len(str(step).split('.')[-1]) if '.' in str(step) else 0
                    qty_str = f"{qty:.{decimals}f}"
                    
                    logging.info(f"BackpackTrader | Attempting order with quantity={qty_str}, price={price}")
                    result = self.auth.execute_order(
                        orderType="Market",
                        side=side,
                        symbol=symbol,
                        quantity=qty_str,
                        reduceOnly=False,
                        autoBorrow=True,
                        autoBorrowRepay=True,
                        autoLend=True,
                        autoLendRedeem=True,
                        selfTradePrevention="RejectTaker"
                    )
                    logging.info(f"BackpackTrader | Placed {side} order for {symbol} with quantity={qty_str}")
                    return True
                except Exception as e2:
                    logging.error(f"BackpackTrader | quantity order error (попытка {attempt+1}/{retry_attempts}): {e2}")
                
                    # Если это не последняя глобальная попытка, делаем паузу согласно конфигу
                    if attempt < retry_attempts - 1:
                        delay = random.uniform(min_delay, max_delay)
                        logging.info(f"BackpackTrader | Ожидание {delay:.1f}с перед следующей попыткой")
                        time.sleep(delay)
            
        return False  # Если все попытки не удались
                    
    def place_long_with_full_margin(self, symbol: str, leverage: float = 1.0) -> bool:
        return self.execute_full_margin_order(symbol, "Bid", leverage)

    def place_short_with_full_margin(self, symbol: str, leverage: float = 1.0) -> bool:
        return self.execute_full_margin_order(symbol, "Ask", leverage)
    
# Constants
USDC = "USDC"
BLOCKCHAIN = "Solana"
LEVERAGE_DEFAULT = 50

class SubAccount:
    def __init__(self, cfg: Dict, is_long: bool, leverage: float):
        self.name = cfg["name"]
        self.address = cfg["address"]
        self.is_long = is_long
        self.leverage = leverage
        self.trader = BackpackTrader(cfg["api_key"], cfg["api_secret"])
        self.min_delay = 1.0
        self.max_delay = 1.0
        self.retry_attempts = 8  # Maximum number of attempts to open a position

    def random_delay(self):
        """Execute a random delay between min_delay and max_delay seconds."""
        delay = random.uniform(self.min_delay, self.max_delay)
        logging.info(f"{self.name} | Delaying for {delay:.1f}s")
        time.sleep(delay)

    def open_position(self, symbol: str) -> bool:
        """Open a position with retries and delays."""
        for attempt in range(self.retry_attempts):
            logging.info(f"{self.name} | Opening position attempt {attempt+1}/{self.retry_attempts}")
            if self.is_long:
                success = self.trader.execute_full_margin_order(
                    symbol, 
                    "Bid", 
                    self.leverage, 
                    retry_attempts=self.retry_attempts,
                    min_delay=self.min_delay,
                    max_delay=self.max_delay
                )
            else:
                success = self.trader.execute_full_margin_order(
                    symbol, 
                    "Ask", 
                    self.leverage,
                    retry_attempts=self.retry_attempts,
                    min_delay=self.min_delay,
                    max_delay=self.max_delay
                )
                    
            if success:
                logging.info(f"{self.name} | Position opened successfully")
                return True
                    
            if attempt < self.retry_attempts - 1:
                delay = random.uniform(self.min_delay, self.max_delay)  # Используем задержку из конфига
                logging.info(f"{self.name} | Retrying after {delay:.1f}s...")
                time.sleep(delay)
                    
        logging.error(f"{self.name} | Failed to open position after {self.retry_attempts} attempts")
        return False

    def has_position(self, symbol: str) -> bool:
        """Проверка наличия позиции с использованием правильного эндпоинта."""
        try:
            # Используем правильный эндпоинт, как в репозитории 0xCherryDAO/backpack
            positions_response = self.trader.auth._send_request("GET", "api/v1/position", "positionQuery", {})
            
            # Преобразуем ответ в список позиций
            positions = []
            if isinstance(positions_response, dict) and "data" in positions_response:
                positions = positions_response["data"]
            elif isinstance(positions_response, list):
                positions = positions_response
            
            # Логируем общее количество найденных позиций
            if positions:
                logging.info(f"{self.name} | Найдено {len(positions)} позиций")
                
                # Проверяем минимальное соответствие - просто наличие позиции с правильным символом
                for pos in positions:
                    pos_symbol = pos.get("symbol", "")
                    
                    # Проверка всех вариантов символа
                    symbol_variants = [
                        symbol,
                        symbol.replace("_", "-"),
                        symbol.replace("-", "_")
                    ]
                    
                    if pos_symbol in symbol_variants:
                        # Если символ совпадает, считаем что позиция существует
                        
                        # Попытаемся получить дополнительную информацию, если она есть
                        pos_size = pos.get("netQuantity", pos.get("size", "Неизвестно"))
                        entry_price = pos.get("entryPrice", "Неизвестно")
                        mark_price = pos.get("markPrice", entry_price)
                        liq_price = pos.get("estLiquidationPrice", "Неизвестно")
                        pnl = pos.get("unrealizedPnl", "Неизвестно")
                        
                        # Расчет размера в долларах
                        size_dollars = "Неизвестно"
                        try:
                            if pos_size != "Неизвестно" and entry_price != "Неизвестно":
                                size_value = float(pos_size)
                                entry_value = float(entry_price)
                                size_dollars = abs(size_value * entry_value)
                                size_dollars = f"{size_dollars:.2f} USDC"
                        except:
                            pass
                            
                        # Более понятное отображение направления позиции
                        side_txt = "LONG" if str(pos_size).startswith('3') or str(pos_size).startswith('+') else "SHORT"
                        
                        logging.info(f"{self.name} | {side_txt} ПОЗИЦИЯ: {pos_symbol}, размер={pos_size} (~{size_dollars}), вход={entry_price}, тек.цена={mark_price}, ликв.={liq_price}, PnL={pnl}")
                        return True
                
                # Если ни одна позиция не подходит по символу
                logging.info(f"{self.name} | Не найдено позиций для символа {symbol}")
                return False
            else:
                logging.info(f"{self.name} | Позиции не найдены")
                return False
            
        except Exception as e:
            logging.warning(f"{self.name} | Ошибка при проверке позиций: {e}")
            return False
    
    def close_position(self, symbol: str) -> bool:
        """Закрытие позиции с использованием рабочего метода."""
        if not self.has_position(symbol):
            logging.info(f"{self.name} | Нет позиции для закрытия")
            return True  # Нет позиции - считаем успешным закрытием
            
        side = "Ask" if self.is_long else "Bid"  # Bid для покупки, Ask для продажи
        
        try:
            # Запрос на получение информации о позиции для определения её размера
            positions = self.trader.auth._send_request("GET", "api/v1/position", "positionQuery", {})
            
            for pos in positions:
                if pos.get("symbol") == symbol:
                    # Получаем размер позиции
                    size = pos.get("netQuantity", "0")
                    if size:
                        try:
                            # Создаем ордер с указанием размера позиции
                            result = self.trader.auth.execute_order(
                                orderType="Market",
                                side=side,
                                symbol=symbol,
                                reduceOnly=True,
                                quantity=str(abs(float(size)))
                            )
                            logging.info(f"{self.name} | Позиция закрыта успешно: размер={size}")
                            return True
                        except Exception as e:
                            logging.error(f"{self.name} | Ошибка закрытия позиции: {e}")
        except Exception as e:
            logging.error(f"{self.name} | Ошибка при получении данных о позиции: {e}")
        
        # Если не удалось получить размер или закрыть позицию, пробуем с фиксированным размером
        try:
            # Пробуем с фиксированным размером (обычно работает с обычными значениями)
            fixed_size = "3.58"  # Типичный размер для SOL с депозитом 10 USDC и леверейджем 50x
            result = self.trader.auth.execute_order(
                orderType="Market",
                side=side,
                symbol=symbol,
                reduceOnly=True,
                quantity=fixed_size
            )
            logging.info(f"{self.name} | Позиция закрыта с фиксированным размером {fixed_size}")
            return True
        except Exception as e:
            logging.error(f"{self.name} | Не удалось закрыть позицию: {e}")
            return False

    def sweep(self, main_address: str) -> bool:
        """Withdraw all available funds to the main account."""
        balance = self.trader.get_available_margin()
        if balance <= 0.1:  # Minimum threshold for withdrawal
            logging.info(f"{self.name} | Insufficient funds to sweep: {balance} USDC")
            return True  # Consider successful if nothing to withdraw
            
        # Round to 6 decimal places
        qty = round(balance, 6)
        qty_str = f"{qty:.6f}"
        
        for attempt in range(3):  # Multiple withdrawal attempts
            try:
                result = self.trader.auth.request_withdrawal(
                    address=main_address,
                    blockchain=BLOCKCHAIN,
                    quantity=qty_str,
                    symbol=USDC
                )
                logging.info(f"{self.name} | Swept {qty_str} USDC to main")
                return True
            except Exception as e:
                logging.warning(f"{self.name} | Error sweeping funds (attempt {attempt+1}): {e}")
                if "Insufficient collateral" in str(e):
                    logging.info(f"{self.name} | Funds may be locked in position, trying smaller amount")
                    # Try withdrawing a smaller amount
                    try:
                        qty *= 0.5  # Half the amount
                        if qty < 0.1:  # If too small, stop
                            return False
                        qty_str = f"{qty:.6f}"
                        result = self.trader.auth.request_withdrawal(
                            address=main_address,
                            blockchain=BLOCKCHAIN,
                            quantity=qty_str,
                            symbol=USDC
                        )
                        logging.info(f"{self.name} | Swept reduced amount {qty_str} USDC to main")
                        return True
                    except Exception as e2:
                        logging.error(f"{self.name} | Failed to sweep reduced amount: {e2}")
                        
                if attempt < 2:
                    time.sleep(1)
                    
        logging.error(f"{self.name} | Failed to sweep funds after multiple attempts")
        return False


def worker_pair(short_cfg: Dict, long_cfg: Dict, cfg: Dict, main_address: str, delay_start: float = 0) -> None:
    """
    Функция обработки пары аккаунтов с возможностью начальной задержки.
    
    :param delay_start: Начальная задержка перед стартом пары (в секундах)
    """
    # Добавляем начальную задержку для распределения нагрузки
    if delay_start > 0:
        logging.info(f"Пара {short_cfg['name']}/{long_cfg['name']} ожидает {delay_start:.1f}с перед запуском")
        time.sleep(delay_start)
        
    short_acc = SubAccount(short_cfg, is_long=False, leverage=float(cfg.get("leverage", LEVERAGE_DEFAULT)))
    long_acc = SubAccount(long_cfg, is_long=True, leverage=float(cfg.get("leverage", LEVERAGE_DEFAULT)))
    
    # Остальной код функции worker_pair остается без изменений
    # ...
    
    # Настройка задержек из конфигурации
    action_cfg = cfg.get("action_delay", {})
    short_acc.min_delay = float(action_cfg.get("min", 1))
    short_acc.max_delay = float(action_cfg.get("max", short_acc.min_delay))
    long_acc.min_delay = short_acc.min_delay
    long_acc.max_delay = short_acc.max_delay
    
    parent = AuthenticationClient(cfg["api"]["key"], cfg["api"]["secret"])
    symbol = cfg["symbol"]
    check_interval = float(cfg.get("check_interval", 10))
    deposit_amt = float(cfg.get("initial_deposit", 0))
    
    while True:
        # Сбрасываем флаги для нового цикла
        short_position_opened = False
        long_position_opened = False
        
        try:
            # 1. Депозит на оба суб-аккаунта с задержками между депозитами
            logging.info(f"Starting new cycle with {deposit_amt} USDC deposits")
            
            # Депозит на short аккаунт
            try:
                parent.request_withdrawal(
                    address=short_acc.address,
                    blockchain=BLOCKCHAIN,
                    quantity=f"{deposit_amt:.6f}",
                    symbol=USDC
                )
                logging.info(f"{short_acc.name} | Deposited {deposit_amt:.6f} USDC")
            except Exception as e:
                logging.error(f"{short_acc.name} | Deposit error: {e}")
            
            # Задержка между депозитами согласно конфигу
            delay = random.uniform(short_acc.min_delay, short_acc.max_delay)
            logging.info(f"Delaying {delay:.1f}s between deposits")
            time.sleep(delay)
            
            # Депозит на long аккаунт
            try:
                parent.request_withdrawal(
                    address=long_acc.address,
                    blockchain=BLOCKCHAIN,
                    quantity=f"{deposit_amt:.6f}",
                    symbol=USDC
                )
                logging.info(f"{long_acc.name} | Deposited {deposit_amt:.6f} USDC")
            except Exception as e:
                logging.error(f"{long_acc.name} | Deposit error: {e}")
            
            # 2. Случайная задержка перед открытием позиций (из конфига)
            delay = random.uniform(short_acc.min_delay, short_acc.max_delay)
            logging.info(f"Delaying {delay:.1f}s before opening positions")
            time.sleep(delay)
            
            # 3. Открытие позиций на обоих аккаунтах
            short_position_opened = short_acc.open_position(symbol)
            long_position_opened = long_acc.open_position(symbol)

            # Проверяем, открылась ли хотя бы одна позиция
            if not (short_position_opened or long_position_opened):
                logging.error("Failed to open positions on both accounts, restarting cycle")
                # Попытка вывести средства перед перезапуском цикла
                short_acc.sweep(main_address)
                long_acc.sweep(main_address)
                time.sleep(3)
                continue

            # Add a force_monitoring flag for cases where positions might not be visible via API
            force_monitoring = False

            # Используем задержку из конфига перед началом мониторинга
            if short_position_opened or long_position_opened:
                logging.info("Позиции открыты, ожидаем перед началом мониторинга")
                # Используем задержку из конфига (action_delay)
                delay = random.uniform(short_acc.min_delay, short_acc.max_delay)
                logging.info(f"Задержка {delay:.1f}с перед проверкой позиций")
                time.sleep(delay)

                # Проверяем наличие позиций
                short_visible = short_acc.has_position(symbol) if short_position_opened else False
                long_visible = long_acc.has_position(symbol) if long_position_opened else False

                logging.info(f"Статус позиций - SHORT: {'видна' if short_visible else 'не видна'}, LONG: {'видна' if long_visible else 'не видна'}")
                
                # Сразу начинаем мониторинг, если позиции видны
                if short_visible or long_visible:
                    logging.info(f"Начинаем мониторинг позиций с интервалом {check_interval}с")
                    
                # 4. Мониторинг позиций на предмет ликвидации
                liquidation_detected = False
                monitoring_start_time = time.time()
                max_monitoring_time = 3600 * 24  # 24 часа максимального мониторинга
                    
                while time.time() - monitoring_start_time < max_monitoring_time:
                        # Продолжение мониторинга...
                    short_has_position = short_acc.has_position(symbol)
                    long_has_position = long_acc.has_position(symbol)
                    
                    logging.info(f"Результат проверки позиций - SHORT: {short_has_position}, LONG: {long_has_position}")
                    
                    # Проверка на ликвидацию
                    if short_position_opened and not short_has_position:
                        logging.info(f"Короткая позиция ликвидирована или закрыта")
                        liquidation_detected = True
                        break
                        
                    if long_position_opened and not long_has_position:
                        logging.info(f"Длинная позиция ликвидирована или закрыта")
                        liquidation_detected = True
                        break
                    
                    # Если обе позиции существуют, продолжаем мониторинг
                    if short_has_position and long_has_position:
                        logging.info(f"Обе позиции активны, продолжаем мониторинг")
                        time.sleep(check_interval)
                    # Если ни одной позиции не осталось (странная ситуация)
                    elif not short_has_position and not long_has_position:
                        logging.warning(f"Обе позиции исчезли, завершаем мониторинг")
                        liquidation_detected = True
                        break
                    else:
                        # Одна позиция исчезла - это ликвидация
                        logging.info(f"Одна позиция ликвидирована, завершаем мониторинг")
                        liquidation_detected = True
                        break
            
            # 5. Закрытие выживших позиций
            if short_position_opened and short_acc.has_position(symbol):
                logging.info(f"Closing surviving short position")
                short_acc.close_position(symbol)
                
            if long_position_opened and long_acc.has_position(symbol):
                logging.info(f"Closing surviving long position")
                long_acc.close_position(symbol)
            
            # Задержка перед выводом средств
            delay = random.uniform(short_acc.min_delay, short_acc.max_delay)
            logging.info(f"Delaying {delay:.1f}s before sweeping funds")
            time.sleep(delay)
            
            # 6. Свип средств на основной счет
            logging.info(f"Sweeping funds back to main account")
            short_acc.sweep(main_address)
            long_acc.sweep(main_address)
            
            # Задержка перед следующим циклом
            delay = random.uniform(short_acc.min_delay, short_acc.max_delay)
            logging.info(f"Delaying {delay:.1f}s before starting next cycle")
            time.sleep(delay)
            
        except Exception as e:
            logging.error(f"Critical error in worker cycle: {e}")
            # В случае критической ошибки пытаемся закрыть позиции и вывести средства
            try:
                if short_acc.has_position(symbol):
                    short_acc.close_position(symbol)
                if long_acc.has_position(symbol):
                    long_acc.close_position(symbol)
                short_acc.sweep(main_address)
                long_acc.sweep(main_address)
            except Exception as cleanup_error:
                logging.error(f"Error during error cleanup: {cleanup_error}")
            time.sleep(10)  # Подольше ждем перед новой попыткой после критической ошибки

def main():
    """Initialize and run the bot using configuration from config.yaml."""
    # Создаем папку для логов, если её нет
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Имя файла лога с датой и временем
    log_filename = f"backpack_liquidation_{time.strftime('%Y%m%d_%H%M%S')}.log"
    log_path = logs_dir / log_filename
    
    # Setup colored logging
    import colorlog
    
    handler = colorlog.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'red,bg_white',
            }
        )
    )
    
    # Файловый обработчик логов теперь записывает в папку logs
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    logger = colorlog.getLogger()
    logger.addHandler(handler)
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)
    
    logging.info(f"Запуск бота. Логи сохраняются в {log_path}")
    
    # Load configuration
    config_path = Path("config.yaml")
    if not config_path.exists():
        logging.error(f"Configuration file not found: {config_path}")
        sys.exit(1)
        
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    # Validate configuration
    if not config.get("main_account", {}).get("address"):
        logging.error("Main account address not configured")
        sys.exit(1)
        
    pairs = config.get("pairs", [])
    if not pairs:
        logging.error("No trading pairs configured")
        sys.exit(1)
    
    main_address = config["main_account"]["address"]
    logging.info(f"Starting Backpack liquidation bot with {len(pairs)} pair(s)")
    
    # Start worker threads for each pair
    threads = []
    # Максимальная начальная задержка из конфига или 60 секунд по умолчанию
    max_initial_delay = float(config.get("pair_start_delay_max", 60))

    for i, pair_config in enumerate(pairs):
        short_account = pair_config.get("short_account")
        long_account = pair_config.get("long_account")
        
        if not (short_account and long_account):
            logging.warning(f"Skipping pair with missing account configuration")
            continue
            
        # Добавляем случайную начальную задержку для равномерного распределения
        initial_delay = random.uniform(0, max_initial_delay)
        
        thread = threading.Thread(
            target=worker_pair,
            args=(short_account, long_account, config, main_address, initial_delay),
            daemon=True
        )
        thread.start()
        threads.append(thread)
        logging.info(f"Started worker thread for {short_account['name']} / {long_account['name']} with {initial_delay:.1f}s initial delay")
    
    # Wait for all threads
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, shutting down...")
        sys.exit(0)

if __name__ == "__main__":
    main()
