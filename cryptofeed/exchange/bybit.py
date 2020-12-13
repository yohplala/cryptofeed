'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import logging
from decimal import Decimal

from sortedcontainers import SortedDict as sd
from yapic import json

from cryptofeed.defines import BID, ASK, BUY, BYBIT, L2_BOOK, SELL, TRADES, OPEN_INTEREST, FUTURES_INDEX
from cryptofeed.feed import Feed
from cryptofeed.standards import pair_exchange_to_std as normalize_pair
from cryptofeed.standards import timestamp_normalize


LOG = logging.getLogger('feedhandler')


class Bybit(Feed):
    id = BYBIT

    def __init__(self, pairs=None, channels=None, callbacks=None, **kwargs):
        super().__init__('wss://stream.bybit.com/realtime', pairs=pairs, channels=channels, callbacks=callbacks, **kwargs)

    def __reset(self):
        self.l2_book = {}

    async def message_handler(self, msg: str, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)

        if "success" in msg:
            if msg['success']:
                LOG.debug("%s: Subscription success %s", self.id, msg)
            else:
                LOG.error("%s: Error from exchange %s", self.id, msg)
        elif "trade" in msg["topic"]:
            await self._trade(msg, timestamp)
        elif "orderBook" in msg["topic"]:
            await self._book(msg, timestamp)
        elif "instrument_info" in msg["topic"]:
            await self._instrument_info(msg, timestamp)
        else:
            LOG.warning("%s: Invalid message type %s", self.id, msg)

    async def subscribe(self, websocket):
        self.__reset()

        for chan in self.channels if self.channels else self.config:
            for pair in self.pairs if self.pairs else self.config[chan]:
                await websocket.send(json.dumps(
                    {
                        "op": "subscribe",
                        "args": [f"{chan}.{pair}"]
                    }
                ))

    async def _instrument_info(self, msg: dict, timestamp: float):
        """
        ### Snapshot type update
        {
        "topic": "instrument_info.100ms.BTCUSD",
        "type": "snapshot",
        "data": {
            "id": 1,
            "symbol": "BTCUSD",                           //instrument name
            "last_price_e4": 81165000,                    //the latest price
            "last_tick_direction": "ZeroPlusTick",        //the direction of last tick:PlusTick,ZeroPlusTick,MinusTick,
                                                          //ZeroMinusTick
            "prev_price_24h_e4": 81585000,                //the price of prev 24h
            "price_24h_pcnt_e6": -5148,                   //the current last price percentage change from prev 24h price
            "high_price_24h_e4": 82900000,                //the highest price of prev 24h
            "low_price_24h_e4": 79655000,                 //the lowest price of prev 24h
            "prev_price_1h_e4": 81395000,                 //the price of prev 1h
            "price_1h_pcnt_e6": -2825,                    //the current last price percentage change from prev 1h price
            "mark_price_e4": 81178500,                    //mark price
            "index_price_e4": 81172800,                   //index price
            "open_interest": 154418471,                   //open interest quantity - Attention, the update is not
                                                          //immediate - slowest update is 1 minute
            "open_value_e8": 1997561103030,               //open value quantity - Attention, the update is not
                                                          //immediate - the slowest update is 1 minute
            "total_turnover_e8": 2029370141961401,        //total turnover
            "turnover_24h_e8": 9072939873591,             //24h turnover
            "total_volume": 175654418740,                 //total volume
            "volume_24h": 735865248,                      //24h volume
            "funding_rate_e6": 100,                       //funding rate
            "predicted_funding_rate_e6": 100,             //predicted funding rate
            "cross_seq": 1053192577,                      //sequence
            "created_at": "2018-11-14T16:33:26Z",
            "updated_at": "2020-01-12T18:25:16Z",
            "next_funding_time": "2020-01-13T00:00:00Z",  //next funding time
                                                          //the rest time to settle funding fee
            "countdown_hour": 6                           //the remaining time to settle the funding fee
        },
        "cross_seq": 1053192634,
        "timestamp_e6": 1578853524091081                  //the timestamp when this information was produced
        }

        ### Delta type update
        {
        "topic": "instrument_info.100ms.BTCUSD",
        "type": "delta",
        "data": {
            "delete": [],
            "update": [
                {
                    "id": 1,
                    "symbol": "BTCUSD",
                    "prev_price_24h_e4": 81565000,
                    "price_24h_pcnt_e6": -4904,
                    "open_value_e8": 2000479681106,
                    "total_turnover_e8": 2029370495672976,
                    "turnover_24h_e8": 9066215468687,
                    "volume_24h": 735316391,
                    "cross_seq": 1053192657,
                    "created_at": "2018-11-14T16:33:26Z",
                    "updated_at": "2020-01-12T18:25:25Z"
                }
            ],
            "insert": []
        },
        "cross_seq": 1053192657,
        "timestamp_e6": 1578853525691123
        }
        """
        update_type = msg['type']

        if update_type == 'snapshot':
            updates = [msg['data']]
        else:
            updates = msg['data']['update']

        for info in updates:
            if 'open_interest' in info:
                await self.callback(OPEN_INTEREST, feed=self.id,
                                    pair=normalize_pair(info['symbol']),
                                    open_interest=info['open_interest'],
                                    timestamp=timestamp_normalize(self.id, info['updated_at'].timestamp() * 1000),
                                    receipt_timestamp=timestamp)

            if 'index_price_e4' in info:
                await self.callback(FUTURES_INDEX, feed=self.id,
                                    pair=normalize_pair(info['symbol']),
                                    futures_index=info['index_price_e4'] * 1e-4,
                                    timestamp=timestamp_normalize(self.id, info['updated_at'].timestamp() * 1000),
                                    receipt_timestamp=timestamp)

    async def _trade(self, msg: dict, timestamp: float):
        """
        {"topic":"trade.BTCUSD",
        "data":[
            {
                "timestamp":"2019-01-22T15:04:33.461Z",
                "symbol":"BTCUSD",
                "side":"Buy",
                "size":980,
                "price":3563.5,
                "tick_direction":"PlusTick",
                "trade_id":"9d229f26-09a8-42f8-aff3-0ba047b0449d",
                "cross_seq":163261271}]}
        """
        data = msg['data']
        for trade in data:
            await self.callback(TRADES,
                                feed=self.id,
                                pair=normalize_pair(trade['symbol']),
                                order_id=trade['trade_id'],
                                side=BUY if trade['side'] == 'Buy' else SELL,
                                amount=Decimal(trade['size']),
                                price=Decimal(trade['price']),
                                timestamp=timestamp_normalize(self.id, trade['trade_time_ms']),
                                receipt_timestamp=timestamp
                                )

    async def _book(self, msg: dict, timestamp: float):
        pair = normalize_pair(msg['topic'].split('.')[-1])
        update_type = msg['type']
        data = msg['data']
        forced = False
        delta = {BID: [], ASK: []}

        if update_type == 'snapshot':
            self.l2_book[pair] = {BID: sd({}), ASK: sd({})}
            for update in data:
                side = BID if update['side'] == 'Buy' else ASK
                self.l2_book[pair][side][Decimal(update['price'])] = Decimal(update['size'])
            forced = True
        else:
            for delete in data['delete']:
                side = BID if delete['side'] == 'Buy' else ASK
                price = Decimal(delete['price'])
                delta[side].append((price, 0))
                del self.l2_book[pair][side][price]

            for utype in ('update', 'insert'):
                for update in data[utype]:
                    side = BID if update['side'] == 'Buy' else ASK
                    price = Decimal(update['price'])
                    amount = Decimal(update['size'])
                    delta[side].append((price, amount))
                    self.l2_book[pair][side][price] = amount

        # timestamp is in microseconds
        await self.book_callback(self.l2_book[pair], L2_BOOK, pair, forced, delta, msg['timestamp_e6'] / 1000000, timestamp)
