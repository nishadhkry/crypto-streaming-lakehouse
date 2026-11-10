import asyncio
import json
import logging

import websockets
from websockets.exceptions import WebSocketException

log = logging.getLogger(__name__)

URL = 'wss://ws-feed.exchange.coinbase.com'
PRODUCTS = ['BTC-USD', 'ETH-USD', 'SOL-USD']
SUBSCRIBE = {
    'type': 'subscribe',
    'product_ids': PRODUCTS,
    'channels': ['ticker'],
}


def parse_message(raw):
    '''Return the message as a dict if it is a usable price tick, else None.'''
    try:
        msg = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(msg, dict) or msg.get('type') != 'ticker':
        return None
    if not msg.get('product_id') or not msg.get('price'):
        return None
    return msg


async def stream_ticks():
    '''Yield price ticks forever, reconnecting with backoff on failure.'''
    backoff = 1
    while True:
        try:
            async with websockets.connect(URL, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps(SUBSCRIBE))
                log.info('connected and subscribed to %s', PRODUCTS)
                backoff = 1
                async for raw in ws:
                    tick = parse_message(raw)
                    if tick:
                        yield tick
        except (WebSocketException, OSError, asyncio.TimeoutError) as exc:
            log.warning('connection problem: %s', exc)
        log.info('reconnecting in %s seconds', backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


async def _demo():
    count = 0
    async for tick in stream_ticks():
        print(tick['product_id'], tick['price'], tick['time'])
        count += 1
        if count >= 20:
            break


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    asyncio.run(_demo())