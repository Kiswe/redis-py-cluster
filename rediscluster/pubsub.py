# -*- coding: utf-8 -*-

# 3rd party imports
from redis.client import PubSub, PubSubWorkerThread


class ClusterPubSub(PubSub):
    """
    Wrapper for PubSub class.
    """

    def __init__(self, *args, **kwargs):
        super(ClusterPubSub, self).__init__(*args, **kwargs)
        self.p_connections = []
        self.node_flags = {'psubscribe': 'all-masters'}


    def determine_nodes(*args, **kwargs):
        command = args[0]
        node_flag = self.nodes_flags.get(command)
        if node_flags == 'all-masters':
            return self.connection_pool.nodes.all_nodes()

    def psubscribe(self, *args, **kwargs):


    def execute_command(self, *args, **kwargs):
        """
        Execute a publish/subscribe command.
        Taken code from redis-py and tweak to make it work within a cluster.
        """
        # NOTE: don't parse the response in this function -- it could pull a
        # legitimate message off the stack if the connection is already
        # subscribed to one or more channels

        if self.connection is None:
            self.connection = self.connection_pool.get_connection(
                'pubsub', 
                self.shard_hint,
                channel=args[1],
            )
            # register a callback that re-subscribes to any channels we
            # were listening to when we were disconnected
            self.connection.register_connect_callback(self.on_connect)
        connection = self.connection
        self._execute(connection, connection.send_command, *args)

    
class PubSubHendrik(object):
    """
    PubSub provides publish, subscribe and listen support to Redis channels.

    After subscribing to one or more channels, the listen() method will block
    until a message arrives on one of the subscribed channels. That message
    will be returned and it's safe to start listening again.
    """
    PUBLISH_MESSAGE_TYPES = ('message', 'pmessage')
    UNSUBSCRIBE_MESSAGE_TYPES = ('unsubscribe', 'punsubscribe')
    HEALTH_CHECK_MESSAGE = 'redis-py-health-check'

    def __init__(self, connection_pool, shard_hint=None,
                 ignore_subscribe_messages=False):
        self.connection_pool = connection_pool
        self.shard_hint = shard_hint
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self.connections = []
        # we need to know the encoding options for this connection in order
        # to lookup channel and pattern names for callback handlers.
        self.encoder = self.connection_pool.get_encoder()
        if self.encoder.decode_responses:
            self.health_check_response = ['pong', self.HEALTH_CHECK_MESSAGE]
        else:
            self.health_check_response = [
                b'pong',
                self.encoder.encode(self.HEALTH_CHECK_MESSAGE)
            ]
        self.reset()

    def __del__(self):
        try:
            # if this object went out of scope prior to shutting down
            # subscriptions, close the connection manually before
            # returning it to the connection pool
            self.reset()
        except Exception:
            pass

    def reset(self):
        if self.connections:
            for connection in connections:
                self.connection.disconnect()
                self.connection.clear_connect_callbacks()
                self.connection_pool.release(self.connection)
                self.connection = None
        self.channels = {}
        self.pending_unsubscribe_channels = set()
        self.patterns = {}
        self.pending_unsubscribe_patterns = set()

    def close(self):
        self.reset()

    def on_connect(self, connection):
        "Re-subscribe to any channels and patterns previously subscribed to"
        # NOTE: for python3, we can't pass bytestrings as keyword arguments
        # so we need to decode channel/pattern names back to unicode strings
        # before passing them to [p]subscribe.
        self.pending_unsubscribe_channels.clear()
        self.pending_unsubscribe_patterns.clear()
        if self.channels:
            channels = {}
            for k, v in iteritems(self.channels):
                channels[self.encoder.decode(k, force=True)] = v
            self.subscribe(**channels)
        if self.patterns:
            patterns = {}
            for k, v in iteritems(self.patterns):
                patterns[self.encoder.decode(k, force=True)] = v
            self.psubscribe(**patterns)

    @property
    def subscribed(self):
        "Indicates if there are subscriptions to any channels or patterns"
        return bool(self.channels or self.patterns)

    def execute_command(self, *args):
        "Execute a publish/subscribe command"

        # NOTE: don't parse the response in this function -- it could pull a
        # legitimate message off the stack if the connection is already
        # subscribed to one or more channels

        if len(self.connections) == 0:
            self.connections = self.connection_pool.nodes.all_masters()
            # register a callback that re-subscribes to any channels we
            # were listening to when we were disconnected
            for connection in self.connections:
                self.connection.register_connect_callback(self.on_connect)
        connections = self.connections
        kwargs = {'check_health': not self.subscribed}
        self._execute(connection, connection.send_command, *args, **kwargs)

    def _execute(self, connections, command, *args, **kwargs):
        results = []
        for connection in connections:
            try:
                results.append(command(*args, **kwargs))
            except (ConnectionError, TimeoutError) as e:
                connection.disconnect()
                if not (connection.retry_on_timeout and
                        isinstance(e, TimeoutError)):
                    raise
                # Connect manually here. If the Redis server is down, this will
                # fail and raise a ConnectionError as desired.
                connection.connect()
                # the ``on_connect`` callback should haven been called by the
                # connection to resubscribe us to any channels and patterns we were
                # previously listening to
                results.append(command(*args, **kwargs))
        return results[0]

    def parse_response(self, block=True, timeout=0):
        "Parse the response from a publish/subscribe command"
        responses = []
        conns = self.connections
        if len(conns) == 0:
            raise RuntimeError(
                'pubsub connection not set: '
                'did you forget to call subscribe() or psubscribe()?')

        self.check_health()
        for conn in conns:
            if not block and not conn.can_read(timeout=timeout):
                return None
            response = self._execute(conn, conn.read_response)
            responses.append(response)

            if conn.health_check_interval and \
               response == self.health_check_response(conn):
               # ignore the health check message as user might not expect it
               return None
        return response

    def check_health(self, conn):
        if conn is None:
            raise RuntimeError(
                'pubsub connection not set: '
                'did you forget to call subscribe() or psubscribe()?')

        if conn.health_check_interval and time.time() > conn.next_health_check:
            conn.send_command('PING', self.HEALTH_CHECK_MESSAGE,
                              check_health=False)

    def _normalize_keys(self, data):
        """
        normalize channel/pattern names to be either bytes or strings
        based on whether responses are automatically decoded. this saves us
        from coercing the value for each message coming in.
        """
        encode = self.encoder.encode
        decode = self.encoder.decode
        return {decode(encode(k)): v for k, v in iteritems(data)}

    def psubscribe(self, *args, **kwargs):
        """
        Subscribe to channel patterns. Patterns supplied as keyword arguments
        expect a pattern name as the key and a callable as the value. A
        pattern's callable will be invoked automatically when a message is
        received on that pattern rather than producing a message via
        ``listen()``.
        """
        if args:
            args = list_or_args(args[0], args[1:])
        new_patterns = dict.fromkeys(args)
        new_patterns.update(kwargs)
        ret_val = self.execute_command('PSUBSCRIBE', *iterkeys(new_patterns))
        # update the patterns dict AFTER we send the command. we don't want to
        # subscribe twice to these patterns, once for the command and again
        # for the reconnection.
        new_patterns = self._normalize_keys(new_patterns)
        self.patterns.update(new_patterns)
        self.pending_unsubscribe_patterns.difference_update(new_patterns)
        return ret_val

    def punsubscribe(self, *args):
        """
        Unsubscribe from the supplied patterns. If empty, unsubscribe from
        all patterns.
        """
        if args:
            args = list_or_args(args[0], args[1:])
            patterns = self._normalize_keys(dict.fromkeys(args))
        else:
            patterns = self.patterns
        self.pending_unsubscribe_patterns.update(patterns)
        return self.execute_command('PUNSUBSCRIBE', *args)

    def subscribe(self, *args, **kwargs):
        """
        Subscribe to channels. Channels supplied as keyword arguments expect
        a channel name as the key and a callable as the value. A channel's
        callable will be invoked automatically when a message is received on
        that channel rather than producing a message via ``listen()`` or
        ``get_message()``.
        """
        if args:
            args = list_or_args(args[0], args[1:])
        new_channels = dict.fromkeys(args)
        new_channels.update(kwargs)
        ret_val = self.execute_command('SUBSCRIBE', *iterkeys(new_channels))
        # update the channels dict AFTER we send the command. we don't want to
        # subscribe twice to these channels, once for the command and again
        # for the reconnection.
        new_channels = self._normalize_keys(new_channels)
        self.channels.update(new_channels)
        self.pending_unsubscribe_channels.difference_update(new_channels)
        return ret_val

    def unsubscribe(self, *args):
        """
        Unsubscribe from the supplied channels. If empty, unsubscribe from
        all channels
        """
        if args:
            args = list_or_args(args[0], args[1:])
            channels = self._normalize_keys(dict.fromkeys(args))
        else:
            channels = self.channels
        self.pending_unsubscribe_channels.update(channels)
        return self.execute_command('UNSUBSCRIBE', *args)

    def listen(self):
        "Listen for messages on channels this client has been subscribed to"
        while self.subscribed:
            response = self.handle_message(self.parse_response(block=True))
            if response is not None:
                yield response

    def get_message(self, ignore_subscribe_messages=False, timeout=0):
        """
        Get the next message if one is available, otherwise None.

        If timeout is specified, the system will wait for `timeout` seconds
        before returning. Timeout should be specified as a floating point
        number.
        """
        response = self.parse_response(block=False, timeout=timeout)
        if response:
            return self.handle_message(response, ignore_subscribe_messages)
        return None

    def ping(self, message=None):
        """
        Ping the Redis server
        """
        message = '' if message is None else message
        return self.execute_command('PING', message)

    def handle_message(self, response, ignore_subscribe_messages=False):
        """
        Parses a pub/sub message. If the channel or pattern was subscribed to
        with a message handler, the handler is invoked instead of a parsed
        message being returned.
        """
        message_type = nativestr(response[0])
        if message_type == 'pmessage':
            message = {
                'type': message_type,
                'pattern': response[1],
                'channel': response[2],
                'data': response[3]
            }
        elif message_type == 'pong':
            message = {
                'type': message_type,
                'pattern': None,
                'channel': None,
                'data': response[1]
            }
        else:
            message = {
                'type': message_type,
                'pattern': None,
                'channel': response[1],
                'data': response[2]
            }

        # if this is an unsubscribe message, remove it from memory
        if message_type in self.UNSUBSCRIBE_MESSAGE_TYPES:
            if message_type == 'punsubscribe':
                pattern = response[1]
                if pattern in self.pending_unsubscribe_patterns:
                    self.pending_unsubscribe_patterns.remove(pattern)
                    self.patterns.pop(pattern, None)
            else:
                channel = response[1]
                if channel in self.pending_unsubscribe_channels:
                    self.pending_unsubscribe_channels.remove(channel)
                    self.channels.pop(channel, None)

        if message_type in self.PUBLISH_MESSAGE_TYPES:
            # if there's a message handler, invoke it
            if message_type == 'pmessage':
                handler = self.patterns.get(message['pattern'], None)
            else:
                handler = self.channels.get(message['channel'], None)
            if handler:
                handler(message)
                return None
        elif message_type != 'pong':
            # this is a subscribe/unsubscribe message. ignore if we don't
            # want them
            if ignore_subscribe_messages or self.ignore_subscribe_messages:
                return None

        return message

    def run_in_thread(self, sleep_time=0, daemon=False):
        for channel, handler in iteritems(self.channels):
            if handler is None:
                raise PubSubError("Channel: '%s' has no handler registered" %
                                  channel)
        for pattern, handler in iteritems(self.patterns):
            if handler is None:
                raise PubSubError("Pattern: '%s' has no handler registered" %
                                  pattern)

        thread = PubSubWorkerThread(self, sleep_time, daemon=daemon)
        thread.start()
        return thread

