import asyncio
import itertools
import time
from typing import List
from ready_trader_go import BaseAutoTrader, Instrument, Lifespan, MAXIMUM_ASK, MINIMUM_BID, Side


LOT_SIZE = 20
POSITION_LIMIT = 100
TICK_SIZE_IN_CENTS = 100
MIN_BID_NEAREST_TICK = (
    MINIMUM_BID + TICK_SIZE_IN_CENTS) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
MAX_ASK_NEAREST_TICK = MAXIMUM_ASK // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
HEDGE_TIME_LIMIT = 60  # seconds


class AutoTrader(BaseAutoTrader):
    def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
        super().__init__(loop, team_name, secret)
        self.order_ids = itertools.count(1)
        self.bids = set()
        self.asks = set()
        self.ask_id = self.ask_price = self.bid_id = self.bid_price = self.position = 0
        self.order_timestamps = {}

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
        # add timestamp to order
        self.order_timestamps[client_order_id] = self.event_loop.time()
        if client_order_id in self.bids:
            self.position += volume
            if self.position > POSITION_LIMIT:
                # cancel the trade
                self.send_cancel_order(client_order_id)
                self.position -= volume
            else:
                self.send_hedge_order(next(self.order_ids),
                                      Side.ASK, MIN_BID_NEAREST_TICK, volume)

        elif client_order_id in self.asks:
            self.position -= volume
            if self.position < -POSITION_LIMIT:
                # cancel the trade
                self.send_cancel_order(client_order_id)
                self.position += volume

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
            # It could be either a bid or an ask
            self.bids.discard(client_order_id)
            self.asks.discard(client_order_id)
        else:
            hedge_time = self.event_loop.time() - self.order_timestamps.get(client_order_id, 0)
            if hedge_time > HEDGE_TIME_LIMIT:
                self.send_cancel_order(client_order_id)
                if client_order_id == self.bid_id:
                    self.bid_id = 0
                    self.bid_price = 0
                elif client_order_id == self.ask_id:
                    self.ask_id = 0
                    self.ask_price = 0
                self.bids.discard(client_order_id)
                self.asks.discard(client_order_id)
                # self.logger.warning(
                #     "cancelled order %d due to exceeding hedge time limit of %d seconds", client_order_id, HEDGE_TIME_LIMIT)

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
            spread = ask_prices[0] - bid_prices[0]
            fair_price = mid_price + (position / 100) * spread / 2

            # Make sure the fair price is within the allowable range
            fair_price = min(MAX_ASK_NEAREST_TICK, max(
                MIN_BID_NEAREST_TICK, fair_price))

            # Adjust the bid and ask prices based on the fair price
            bid_price = fair_price - spread / 2
            ask_price = fair_price + spread / 2

            # Make sure the bid and ask prices are within the allowable range
            bid_price = min(MAX_ASK_NEAREST_TICK - spread,
                            max(MIN_BID_NEAREST_TICK, bid_price)) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
            ask_price = min(MAX_ASK_NEAREST_TICK, max(
                MIN_BID_NEAREST_TICK + spread, ask_price)) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS

            # Cancel existing orders if necessary
            if self.bid_id != 0 and bid_price not in (self.bid_price, 0):
                self.send_cancel_order(self.bid_id)
                self.bid_id = 0
            if self.ask_id != 0 and ask_price not in (self.ask_price, 0):
                self.send_cancel_order(self.ask_id)
                self.ask_id = 0

            # Place new orders if necessary
            if self.bid_id == 0 and self.position < POSITION_LIMIT:
                max_order_size = max(0, min(POSITION_LIMIT - self.position, ask_volume))
                self.bid_id = next(self.order_ids)
                self.bid_price = bid_price
                self.send_insert_order(
                    self.bid_id, Side.BUY, int(bid_price), max_order_size, Lifespan.FILL_AND_KILL)
                self.bids.add(self.bid_id)
                print(self.position)

            if self.ask_id == 0 and self.position > -POSITION_LIMIT:
                max_order_size = max(0, min(POSITION_LIMIT + self.position, bid_volume))
                self.ask_id = next(self.order_ids)
                self.ask_price = ask_price
                self.send_insert_order(
                    self.ask_id, Side.SELL, int(ask_price), max_order_size, Lifespan.FILL_AND_KILL)
                self.asks.add(self.ask_id)
                print(self.position)

    # ETF_mid = 0
    # FUTURE_mid = 0

    # def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
    #                                 ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
    #     """Called periodically to report the status of an order book.

    #     The sequence number can be used to detect missed or out-of-order
    #     messages. The five best available ask (i.e. sell) and bid (i.e. buy)
    #     prices are reported along with the volume available at each of those
    #     price levels.
    #     """
    #     self.logger.info("received order book for instrument %d with sequence number %d", instrument,
    #                     sequence_number)

    #     if instrument == Instrument.FUTURE:
    #         # price_adjustment = - (self.position // LOT_SIZE) * TICK_SIZE_IN_CENTS
    #         # new_bid_price = bid_prices[0] + price_adjustment if bid_prices[0] != 0 else 0
    #         # new_ask_price = ask_prices[0] + price_adjustment if ask_prices[0] != 0 else 0
    #         self.FUTURE_mid = (ask_prices[0] + bid_prices[0]) // 2
    #     elif instrument == Instrument.ETF:
    #         self.ETF_mid = (ask_prices[0] + bid_prices[0]) // 2

    #     print(self.position)
    #     if self.ETF_mid != 0 and self.FUTURE_mid !=0 and self.ETF_mid < self.FUTURE_mid and self.position < POSITION_LIMIT:
    #         if self.position > 0:
    #             self.ask_id = next(self.order_ids)
    #             self.send_insert_order(self.ask_id, Side.SELL, bid_prices[0], self.position, Lifespan.GOOD_FOR_DAY)
    #             self.asks.add(self.ask_id)
    #         elif self.position < 0:
    #             self.bid_id = next(self.order_ids)
    #             self.send_insert_order(self.bid_id, Side.BUY, ask_prices[0], -self.position, Lifespan.GOOD_FOR_DAY)
    #             self.asks.add(self.ask_id)
    #         self.bid_id = next(self.order_ids)
    #         self.send_insert_order(self.bid_id, Side.BUY, ask_prices[0], 1, Lifespan.FILL_AND_KILL)
    #         self.bids.add(self.bid_id)
    #     elif self.ETF_mid !=0 and self.FUTURE_mid !=0 and self.ETF_mid > self.FUTURE_mid and self.position > -POSITION_LIMIT:
    #         if self.position > 0:
    #             self.bid_id = next(self.order_ids)
    #             self.send_insert_order(self.bid_id, Side.SELL, bid_prices[0], self.position, Lifespan.GOOD_FOR_DAY)
    #             self.bids.add(self.bid_id)
    #         elif self.position < 0:
    #             self.ask_id = next(self.order_ids)
    #             self.send_insert_order(self.ask_id, Side.BUY, ask_prices[0], -self.position, Lifespan.GOOD_FOR_DAY)
    #             self.asks.add(self.ask_id)
    #         self.ask_id = next(self.order_ids)
    #         self.send_insert_order(self.ask_id, Side.SELL, bid_prices[0], 1, Lifespan.FILL_AND_KILL)
    #         self.asks.add(self.ask_id)
