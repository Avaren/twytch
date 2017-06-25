import asyncio
import enum

import websockets
import json

MAX_SUBSCRIPTIONS = 5


class TwitchMessageType(enum.Enum):
    PING = 0
    PONG = 1
    RESPONSE = 2
    RECONNECT = 3
    LISTEN = 4
    UNLISTEN = 5
    MESSAGE = 6


class TwytchPubSub(websockets.client.WebSocketClientProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subscriptions = set()
        self.pong_event = None
        self.is_closed = asyncio.Event()
        self.ping_ft = asyncio.ensure_future(self.ping(), loop=self.loop)
        self.dispatch = None

    async def poll_event(self):
        msg = await self.recv()
        await self.parse_message(msg)

    async def ping(self):
        print('Waitin for hanshake')
        await self.opening_handshake
        print('Handshake don {}')
        while not self.is_closed.is_set():
            print('Do a ping')
            await self.send(json.dumps({"type": TwitchMessageType.PING.name}))
            self.pong_event = self.loop.create_future()
            try:
                await asyncio.wait_for(self.pong_event, timeout=20)
            except asyncio.TimeoutError:
                print('no pong')
            else:
                print('got pong')
            await asyncio.sleep(4 * 60)

    async def parse_message(self, msg: str):
        msg = json.loads(msg)
        print(msg)
        type_ = TwitchMessageType[msg.get('type')]
        if type_ == TwitchMessageType.PONG:
            self.pong_event.set_result(True)
            return

        if type_ == TwitchMessageType.MESSAGE:
            await self.dispatch(msg['data']['topic'], json.loads(msg['data']['message']))

    async def listen(self, topics):
        print('Listening to topics {}'.format(topics))
        self.subscriptions.update(topics)
        await self.send(
            json.dumps({'type': 'LISTEN', 'data': {"topics": topics, 'auth_token': 'kihr41ro66ri3w65v619dgn6byp0ho'}}))

    async def unlisten(self, topics):
        print('Unlistening to topics {}'.format(topics))
        await self.send(json.dumps({'type': 'LISTEN', 'data': {"topics": topics}}))
        self.subscriptions.difference_update(topics)
        if not self.subscriptions:
            print('No more subs, terminating conn')
            await self.close()
            return False
        return True

    async def close(self, code=1000, reason=''):
        self.is_closed.set()
        self.ping_ft.cancel()
        return await super(TwytchPubSub, self).close(code=code, reason=reason)


class Twytch:
    def __init__(self, loop: asyncio.AbstractEventLoop = None):
        self.subscriptions = {}
        self.connections = []
        self.loop = loop or asyncio.get_event_loop()
        self._closing = asyncio.Event()
        self._has_connections = asyncio.Event()

    async def _subscribe(self, topic: str, callback):
        print('Subbing to topic {}'.format(topic))
        sub = self.subscriptions.get(topic)
        if sub:
            sub.append(callback)
            return

        conn = await self._get_connection()
        await conn.listen([topic])
        self.subscriptions[topic] = [callback]

    async def _subscribe_all(self, topics: set, callback):
        print('Subbing to topics {}'.format(topics))
        existing_subs = topics.intersection(self.subscriptions.keys())
        for sub in existing_subs:
            self.subscriptions[sub].append(callback)

        subs = list(topics - existing_subs)
        while subs:
            conn = await self._get_connection()
            num = MAX_SUBSCRIPTIONS - len(conn.subscriptions)
            to_listen = subs[:num]
            subs = subs[num:]
            await conn.listen(to_listen)
            for topic in to_listen:
                self.subscriptions.setdefault(topic, []).append(callback)

    # FIXME: Currently removes entire topic subscription not just specific callback
    async def _unsubscribe(self, topic: str):
        print('Unsubbing to topic {}'.format(topic))
        conn = self._find_conn_with_topic(topic)
        if conn is None:
            print('ERROR: Sub not found')
            return
        self.subscriptions.pop(topic)
        result = await conn.unlisten([topic])
        if result is False:  # no more subs
            print('No more subs, removing conn')
            self.connections.remove(conn)
        if not self.connections:
            self._has_connections.clear()

    def _find_conn_with_topic(self, topic) -> TwytchPubSub:
        for conn in self.connections:
            if topic in conn.subscriptions:
                return conn
        return None

    async def _get_connection(self) -> TwytchPubSub:
        if self.connections:
            for conn in self.connections:
                if len(conn.subscriptions) < MAX_SUBSCRIPTIONS:
                    print('Got conn')
                    return conn
        print('Created conn')
        conn = await self._create_connection()
        self.connections.append(conn)
        self._has_connections.set()
        return conn

    async def _create_connection(self):
        ws = await websockets.connect('wss://pubsub-edge.twitch.tv', loop=self.loop, origin='https://www.twitch.tv',
                                      klass=TwytchPubSub)
        ws.dispatch = self.dispatch
        return ws

    async def sub_user(self, user: str, callback):
        print('Subbing to user {}'.format(user))
        return await self._subscribe('video-playback.{}'.format(user.lower()), callback)

    async def sub_users(self, users: list, callback):
        print('Subbing to users {}'.format(users))
        return await self._subscribe_all({'video-playback.{}'.format(user.lower()) for user in users}, callback)

    async def unsub_user(self, user: str):
        print('Unsubbing to user {}'.format(user))
        return await self._unsubscribe('video-playback.{}'.format(user.lower()))

    async def close(self):
        self._closing.set()
        closes = [conn.close() for conn in self.connections]
        await asyncio.gather(*closes, loop=self.loop)
        self.connections = []

    async def run(self):
        print('Running')
        while not self._closing.is_set():
            await self._has_connections.wait()
            poll = [conn.poll_event() for conn in self.connections]
            done, pending = await asyncio.wait(poll, loop=self.loop, return_when=asyncio.FIRST_COMPLETED)
            for f in done:
                f.result()

    async def dispatch(self, topic, data):
        sub = self.subscriptions.get(topic)
        if sub is None:
            print('No sub for that topic')
            return
        for cb in sub:
            await cb(topic, data)
