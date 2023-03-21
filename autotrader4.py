import asyncio
import itertools
from typing import List
from ready_trader_go import BaseAutoTrader, Instrument, Lifespan, MAXIMUM_ASK, MINIMUM_BID, Side

LOT_SIZE = 20
POSITION_LIMIT = 100
TICK_SIZE_IN_CENTS = 100
MIN_BID_NEAREST_TICK = (
    MINIMUM_BID + TICK_SIZE_IN_CENTS) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
MAX_ASK_NEAREST_TICK = MAXIMUM_ASK // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS


class AutoTrader(BaseAutoTrader):
    def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
        super().__init__(loop, team_name, secret)
        self.order_ids = itertools.count(1)
        self.bids = {}
        self.asks = {}
        self.ask_id = self.ask_price = self.bid_id = self.bid_price = self.position = 0

    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        self.logger.warning("error with order %d: %s",
                            client_order_id, error_message.decode())
        if client_order_id != 0 and (client_order_id in self.bids or client_order_id in self.asks):
            self.on_order_status_message(client_order_id, 0, 0, 0)

    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        self.logger.info("received hedge filled for order %d with average price %d and volume %d", client_order_id,
                         price, volume)

    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        self.logger.info("received order filled for order %d with price %d and volume %d", client_order_id,
                         price, volume)

        if client_order_id in self.bids:
            self.position += volume
            if self.position > POSITION_LIMIT:
                self.position = POSITION_LIMIT
            else:
                self.send_hedge_order(next(self.order_ids),
                                      Side.ASK, MIN_BID_NEAREST_TICK, volume)

        elif client_order_id in self.asks:
            self.position -= volume
            if self.position < -POSITION_LIMIT:
                self.position = -POSITION_LIMIT
            else:
                self.send_hedge_order(next(self.order_ids),
                                      Side.BID, MAX_ASK_NEAREST_TICK, volume)

    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        self.logger.info("received order status for order %d with fill volume %d remaining %d and fees %d",
                         client_order_id, fill_volume, remaining_volume, fees)
        if remaining_volume == 0:
            if client_order_id == self.bid_id:
                self.bid_id = 0
            elif client_order_id == self.ask_id:
                self.ask_id = 0
            self.bids.pop(client_order_id, None)
            self.asks.pop(client_order_id, None)

    def on_trade_ticks_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                               ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        self.logger.info("received trade ticks for instrument %d with sequence number %d", instrument,
                         sequence_number)

    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        self.logger.info("received order book for instrument %d with sequence number %d", instrument,
                         sequence_number)
        if instrument == Instrument.FUTURE:
            bid_volume = bid_volumes[0]
            ask_volume = ask_volumes[0]
            position = self.position // LOT_SIZE
            mid_price = (bid_prices[0] + ask_prices[0]) // 2

            spread = ask_prices[2] - bid_prices[2]

            fair_price = mid_price + (position / 100) * spread / 2

            fair_price = min(MAX_ASK_NEAREST_TICK, max(
                MIN_BID_NEAREST_TICK, fair_price))

            bid_price = fair_price - spread / 2
            ask_price = fair_price + spread / 2

            bid_price = min(MAX_ASK_NEAREST_TICK - spread,
                            max(MIN_BID_NEAREST_TICK, bid_price)) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
            ask_price = min(MAX_ASK_NEAREST_TICK, max(
                MIN_BID_NEAREST_TICK + spread, ask_price)) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS

            if self.bid_id != 0 and bid_price not in (self.bid_price, 0):
                self.send_cancel_order(self.bid_id)
                self.bid_id = 0
            if self.ask_id != 0 and ask_price not in (self.ask_price, 0):
                self.send_cancel_order(self.ask_id)
                self.ask_id = 0

            if self.bid_id == 0 and self.position < POSITION_LIMIT:
                outstanding_bid_volume = sum(
                    order['size'] for order in self.bids.values())
                max_order_size = min(14, max(
                    0, min(POSITION_LIMIT - self.position - outstanding_bid_volume, bid_volume)))
                self.bid_id = next(self.order_ids)
                self.bid_price = bid_price
                self.send_insert_order(self.bid_id, Side.BUY, int(
                    bid_price), max_order_size, Lifespan.GFD)
                self.bids[self.bid_id] = {
                    'price': bid_price, 'size': max_order_size}

            if self.ask_id == 0 and self.position > -POSITION_LIMIT:
                outstanding_ask_volume = sum(
                    order['size'] for order in self.asks.values())
                max_order_size = min(14, max(
                    0, min(POSITION_LIMIT + self.position - outstanding_ask_volume, ask_volume)))
                self.ask_id = next(self.order_ids)
                self.ask_price = ask_price
                self.send_insert_order(self.ask_id, Side.SELL, int(
                    ask_price), max_order_size, Lifespan.GFD)
                self.asks[self.ask_id] = {
                    'price': ask_price, 'size': max_order_size}
