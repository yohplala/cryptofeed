'''
Copyright (C) 2017-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import logging
from decimal import Decimal

from sortedcontainers import SortedDict as sd
from yapic import json
import requests

from cryptofeed.defines import BID, ASK, BITMAX, BUY, L2_BOOK, SELL, TRADES
from cryptofeed.exceptions import MissingSequenceNumber
from cryptofeed.feed import Feed
from cryptofeed.standards import pair_exchange_to_std, pair_std_to_exchange, timestamp_normalize


LOG = logging.getLogger('feedhandler')


class Bitmax(Feed):
    id = BITMAX

    def __init__(self, pairs=None, channels=None, callbacks=None, **kwargs):
        super().__init__('wss://bitmax.io/0/api/pro/v1/stream', pairs=pairs, channels=channels, callbacks=callbacks, **kwargs)

    def __reset(self):
        self.seq_num = {}
        self.l2_book = {}

    async def subscribe(self, websocket):
        self.__reset()
        for channel in self.channels if not self.config else self.config:
            for pair in self.pairs if not self.config else self.config[channel]:
                await websocket.send(json.dumps(
                {
                    "op": "sub",
                    "id": self.hash,
                    "ch": channel.format(pair)
                }
            ))

    async def _trade(self, msg: dict, timestamp: float):
        for trade in msg['trades']:
            await self.callback(TRADES, feed=self.id,
                                pair=pair_exchange_to_std(msg['s']),
                                side=SELL if trade['bm'] else BUY,
                                amount=Decimal(trade['q']),
                                price=Decimal(trade['p']),
                                order_id=None,
                                timestamp=timestamp_normalize(self.id, trade['t']),
                                receipt_timestamp=timestamp)

    def _snapshot(self, pair: str):
        url = f'https://bitmax.io/api/pro/v1/depth?symbol={pair}'
        data = requests.get(url).json()
        data = data['data']
        ts = timestamp_normalize(self.id, data['data']['ts'])
        seq_num = data['data']['seqnum']
        return seq_num

        
    async def _book(self, msg: dict, timestamp: float):
        delta = {BID: [], ASK: []}
        pair = pair_exchange_to_std(msg['symbol'])
        if pair not in self.l2_book:
            sn = self._snapshot(msg['symbol'])
            print(sn)
        print(msg['data']['seqnum'])
        """
        if pair in self.seq_num:
            if msg['data']['seqnum'] != self.seq_num[pair]:
                raise MissingSequenceNumber
        self.seq_num[pair] = msg['data']['seqnum']

        for side in ('bids', 'asks'):
            for price, amount in msg['data'][side]:
                s = BID if side == 'bids' else ASK
                price = Decimal(price)
                size = Decimal(amount)
                if size == 0:
                    delta[s].append((price, 0))
                    if price in self.l2_book[pair][s]:
                        del self.l2_book[pair][s][price]
                else:
                    delta[s].append((price, size))
                    self.l2_book[pair][s][price] = size
        await self.book_callback(self.l2_book[pair], L2_BOOK, pair, True, delta, timestamp_normalize(self.id, msg['data']['ts']), timestamp)
    """
    async def message_handler(self, msg: str, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)
        if 'm' in msg:
            if msg['m'] == 'depth':
                await self._book(msg, timestamp)
            elif msg['m'] == 'marketTrades':
                await self._trade(msg, timestamp)
            elif msg['m'] == 'pong':
                return
            elif msg['m'] == 'connected' or msg['m'] == 'sub':
                return
            else:
                LOG.warning("%s: Invalid message type %s", self.id, msg)
        else:
            LOG.warning("%s: Invalid message type %s", self.id, msg)
