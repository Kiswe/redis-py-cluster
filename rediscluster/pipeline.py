# -*- coding: utf-8 -*-

# python std lib
import sys

# rediscluster imports
from .client import RedisCluster
from .exceptions import (
    RedisClusterException, AskError, MovedError, TryAgainError, ResponseError,
)
from .utils import clusterdown_wrapper, dict_merge

# 3rd party imports
from redis import Redis
from redis.exceptions import ConnectionError, RedisError, TimeoutError
from redis._compat import imap, unicode


ERRORS_ALLOW_RETRY = (ConnectionError, TimeoutError, MovedError, AskError, TryAgainError)


class ClusterPipeline(RedisCluster):
    """
    """

    def __init__(self, connection_pool, result_callbacks=None,
                 response_callbacks=None, startup_nodes=None, read_from_replicas=False):
        """
        """
        self.command_stack = []
        self.refresh_table_asap = False
        self.connection_pool = connection_pool
        self.result_callbacks = result_callbacks or self.__class__.RESULT_CALLBACKS.copy()
        self.startup_nodes = startup_nodes if startup_nodes else []
        self.read_from_replicas = read_from_replicas
        self.nodes_flags = self.__class__.NODES_FLAGS.copy()
        self.response_callbacks = dict_merge(response_callbacks or self.__class__.RESPONSE_CALLBACKS.copy(),
                                             self.CLUSTER_COMMANDS_RESPONSE_CALLBACKS)

    def __repr__(self):
        """
        """
        return "{0}".format(type(self).__name__)

    def __enter__(self):
        """
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        """
        self.reset()

    def __del__(self):
        """
        """
        self.reset()

    def __len__(self):
        """
        """
        return len(self.command_stack)

    def execute_command(self, *args, **kwargs):
        """
        """
        return self.pipeline_execute_command(*args, **kwargs)

    def pipeline_execute_command(self, *args, **options):
        """
        """
        self.command_stack.append(PipelineCommand(args, options, len(self.command_stack)))
        return self

    def raise_first_error(self, stack):
        """
        """
        for c in stack:
            r = c.result
            if isinstance(r, Exception):
                self.annotate_exception(r, c.position + 1, c.args)
                raise r

    def annotate_exception(self, exception, number, command):
        """
        """
        cmd = unicode(' ').join(imap(unicode, command))
        msg = unicode('Command # {0} ({1}) of pipeline caused error: {2}').format(
            number, cmd, unicode(exception.args[0]))
        exception.args = (msg,) + exception.args[1:]

    def execute(self, raise_on_error=True, use_multi = False):
        """
        """
        stack = self.command_stack

        if not stack:
            return []

        try:
            return self.send_cluster_commands(stack, raise_on_error, use_multi = use_multi )
        finally:
            self.reset()

    def reset(self):
        """
        Reset back to empty pipeline.
        """
        self.command_stack = []

        self.scripts = set()

        # TODO: Implement
        # make sure to reset the connection state in the event that we were
        # watching something
        # if self.watching and self.connection:
        #     try:
        #         # call this manually since our unwatch or
        #         # immediate_execute_command methods can call reset()
        #         self.connection.send_command('UNWATCH')
        #         self.connection.read_response()
        #     except ConnectionError:
        #         # disconnect will also remove any previous WATCHes
        #         self.connection.disconnect()

        # clean up the other instance attributes
        self.watching = False
        self.explicit_transaction = False

        # TODO: Implement
        # we can safely return the connection to the pool here since we're
        # sure we're no longer WATCHing anything
        # if self.connection:
        #     self.connection_pool.release(self.connection)
        #     self.connection = None

    @clusterdown_wrapper
    def send_cluster_commands(self, stack, raise_on_error=True, allow_redirections=True, use_multi = False ):
        """
        Send a bunch of cluster commands to the redis cluster.

        `allow_redirections` If the pipeline should follow `ASK` & `MOVED` responses
        automatically. If set to false it will raise RedisClusterException.
        """
        # the first time sending the commands we send all of the commands that were queued up.
        # if we have to run through it again, we only retry the commands that failed.
        attempt = sorted(stack, key=lambda x: x.position)

        # build a list of node objects based on node names we need to
        nodes = {}

        # as we move through each command that still needs to be processed,
        # we figure out the slot number that command maps to, then from the slot determine the node.
        for c in attempt:
            # refer to our internal node -> slot table that tells us where a given
            # command should route to.
            slot = self._determine_slot(*c.args)
            node = self.connection_pool.get_node_by_slot(slot)
            c.slot  = slot

            # little hack to make sure the node name is populated. probably could clean this up.
            self.connection_pool.nodes.set_node_name(node)

            # now that we know the name of the node ( it's just a string in the form of host:port )
            # we can build a list of commands for each node.
            node_name = node['name']
            if node_name not in nodes:
                nodes[node_name] = NodeCommands(
                                        self.parse_response,
                                        self.connection_pool.get_connection_by_node(node),
                                        response_callbacks  = self.response_callbacks,
                                        use_multi           = use_multi )

            nodes[node_name].append(c)

        # send the commands in sequence.
        # we  write to all the open sockets for each node first, before reading anything
        # this allows us to flush all the requests out across the network essentially in parallel
        # so that we can read them all in parallel as they come back.
        # we dont' multiplex on the sockets as they come available, but that shouldn't make too much difference.
        node_commands = nodes.values()
        for n in node_commands:
            n.write()

        for n in node_commands:
            n.read()

        # release all of the redis connections we allocated earlier back into the connection pool.
        # we used to do this step as part of a try/finally block, but it is really dangerous to
        # release connections back into the pool if for some reason the socket has data still left in it
        # from a previous operation. The write and read operations already have try/catch around them for
        # all known types of errors including connection and socket level errors.
        # So if we hit an exception, something really bad happened and putting any of
        # these connections back into the pool is a very bad idea.
        # the socket might have unread buffer still sitting in it, and then the
        # next time we read from it we pass the buffered result back from a previous
        # command and every single request after to that connection will always get
        # a mismatched result. (not just theoretical, I saw this happen on production x.x).
        for n in nodes.values():
            self.connection_pool.release(n.connection)

        # if the response isn't an exception it is a valid response from the node
        # we're all done with that command, YAY!
        # if we have more commands to attempt, we've run into problems.
        # collect all the commands we are allowed to retry.
        # (MOVED, ASK, or connection errors or timeout errors)
        attempt = sorted([c for c in attempt if isinstance(c.result, ERRORS_ALLOW_RETRY)], key=lambda x: x.position)
        if attempt and allow_redirections and not use_multi :
            # RETRY MAGIC HAPPENS HERE!
            # send these remaing comamnds one at a time using `execute_command`
            # in the main client. This keeps our retry logic in one place mostly,
            # and allows us to be more confident in correctness of behavior.
            # at this point any speed gains from pipelining have been lost
            # anyway, so we might as well make the best attempt to get the correct
            # behavior.
            #
            # The client command will handle retries for each individual command
            # sequentially as we pass each one into `execute_command`. Any exceptions
            # that bubble out should only appear once all retries have been exhausted.
            #
            # If a lot of commands have failed, we'll be setting the
            # flag to rebuild the slots table from scratch. So MOVED errors should
            # correct themselves fairly quickly.
            #
            # not use_multi: if use_multi == True, it means we want the commands for
            # the same slot to be atomic.  Automatic retry here spoils the atomic
            # semantic.  So no automatic retry here and let the caller handle the
            # retry, to ensure atomic semantic.
            self.connection_pool.nodes.increment_reinitialize_counter(len(attempt))
            for c in attempt:
                try:
                    # send each command individually like we do in the main client.
                    c.result = super(ClusterPipeline, self).execute_command(*c.args, **c.options)
                except RedisError as e:
                    c.result = e

        # turn the response back into a simple flat array that corresponds
        # to the sequence of commands issued in the stack in pipeline.execute()
        response = [c.result for c in sorted(stack, key=lambda x: x.position)]

        if raise_on_error:
            self.raise_first_error(stack)

        return response

    def _fail_on_redirect(self, allow_redirections):
        """
        """
        if not allow_redirections:
            raise RedisClusterException("ASK & MOVED redirection not allowed in this pipeline")

    def multi(self):
        """
        """
        raise RedisClusterException("method multi() is not implemented")

    def immediate_execute_command(self, *args, **options):
        """
        """
        raise RedisClusterException("method immediate_execute_command() is not implemented")

    def _execute_transaction(self, *args, **kwargs):
        """
        """
        raise RedisClusterException("method _execute_transaction() is not implemented")

    def load_scripts(self):
        """
        """
        raise RedisClusterException("method load_scripts() is not implemented")

    def watch(self, *names):
        """
        """
        raise RedisClusterException("method watch() is not implemented")

    def unwatch(self):
        """
        """
        raise RedisClusterException("method unwatch() is not implemented")

    def script_load_for_pipeline(self, *args, **kwargs):
        """
        """
        raise RedisClusterException("method script_load_for_pipeline() is not implemented")

    def delete(self, *names):
        """
        "Delete a key specified by ``names``"
        """
        if len(names) != 1:
            raise RedisClusterException("deleting multiple keys is not implemented in pipeline command")

        return self.execute_command('DEL', names[0])


def block_pipeline_command(func):
    """
    Prints error because some pipelined commands should be blocked when running in cluster-mode
    """
    def inner(*args, **kwargs):
        raise RedisClusterException("ERROR: Calling pipelined function {0} is blocked when running redis in cluster mode...".format(func.__name__))

    return inner


# Blocked pipeline commands
ClusterPipeline.bgrewriteaof = block_pipeline_command(Redis.bgrewriteaof)
ClusterPipeline.bgsave = block_pipeline_command(Redis.bgsave)
ClusterPipeline.bitop = block_pipeline_command(Redis.bitop)
ClusterPipeline.brpoplpush = block_pipeline_command(Redis.brpoplpush)
ClusterPipeline.client_getname = block_pipeline_command(Redis.client_getname)
ClusterPipeline.client_kill = block_pipeline_command(Redis.client_kill)
ClusterPipeline.client_list = block_pipeline_command(Redis.client_list)
ClusterPipeline.client_setname = block_pipeline_command(Redis.client_setname)
ClusterPipeline.config_get = block_pipeline_command(Redis.config_get)
ClusterPipeline.config_resetstat = block_pipeline_command(Redis.config_resetstat)
ClusterPipeline.config_rewrite = block_pipeline_command(Redis.config_rewrite)
ClusterPipeline.config_set = block_pipeline_command(Redis.config_set)
ClusterPipeline.dbsize = block_pipeline_command(Redis.dbsize)
ClusterPipeline.echo = block_pipeline_command(Redis.echo)
ClusterPipeline.evalsha = block_pipeline_command(Redis.evalsha)
ClusterPipeline.flushall = block_pipeline_command(Redis.flushall)
ClusterPipeline.flushdb = block_pipeline_command(Redis.flushdb)
ClusterPipeline.info = block_pipeline_command(Redis.info)
ClusterPipeline.keys = block_pipeline_command(Redis.keys)
ClusterPipeline.lastsave = block_pipeline_command(Redis.lastsave)
ClusterPipeline.mget = block_pipeline_command(Redis.mget)
ClusterPipeline.move = block_pipeline_command(Redis.move)
ClusterPipeline.mset = block_pipeline_command(Redis.mset)
ClusterPipeline.msetnx = block_pipeline_command(Redis.msetnx)
ClusterPipeline.pfmerge = block_pipeline_command(Redis.pfmerge)
ClusterPipeline.pfcount = block_pipeline_command(Redis.pfcount)
ClusterPipeline.ping = block_pipeline_command(Redis.ping)
ClusterPipeline.publish = block_pipeline_command(Redis.publish)
ClusterPipeline.randomkey = block_pipeline_command(Redis.randomkey)
ClusterPipeline.rename = block_pipeline_command(Redis.rename)
ClusterPipeline.renamenx = block_pipeline_command(Redis.renamenx)
ClusterPipeline.rpoplpush = block_pipeline_command(Redis.rpoplpush)
ClusterPipeline.save = block_pipeline_command(Redis.save)
ClusterPipeline.scan = block_pipeline_command(Redis.scan)
ClusterPipeline.script_exists = block_pipeline_command(Redis.script_exists)
ClusterPipeline.script_flush = block_pipeline_command(Redis.script_flush)
ClusterPipeline.script_kill = block_pipeline_command(Redis.script_kill)
ClusterPipeline.script_load = block_pipeline_command(Redis.script_load)
ClusterPipeline.sdiff = block_pipeline_command(Redis.sdiff)
ClusterPipeline.sdiffstore = block_pipeline_command(Redis.sdiffstore)
ClusterPipeline.sentinel_get_master_addr_by_name = block_pipeline_command(Redis.sentinel_get_master_addr_by_name)
ClusterPipeline.sentinel_master = block_pipeline_command(Redis.sentinel_master)
ClusterPipeline.sentinel_masters = block_pipeline_command(Redis.sentinel_masters)
ClusterPipeline.sentinel_monitor = block_pipeline_command(Redis.sentinel_monitor)
ClusterPipeline.sentinel_remove = block_pipeline_command(Redis.sentinel_remove)
ClusterPipeline.sentinel_sentinels = block_pipeline_command(Redis.sentinel_sentinels)
ClusterPipeline.sentinel_set = block_pipeline_command(Redis.sentinel_set)
ClusterPipeline.sentinel_slaves = block_pipeline_command(Redis.sentinel_slaves)
ClusterPipeline.shutdown = block_pipeline_command(Redis.shutdown)
ClusterPipeline.sinter = block_pipeline_command(Redis.sinter)
ClusterPipeline.sinterstore = block_pipeline_command(Redis.sinterstore)
ClusterPipeline.slaveof = block_pipeline_command(Redis.slaveof)
ClusterPipeline.slowlog_get = block_pipeline_command(Redis.slowlog_get)
ClusterPipeline.slowlog_len = block_pipeline_command(Redis.slowlog_len)
ClusterPipeline.slowlog_reset = block_pipeline_command(Redis.slowlog_reset)
ClusterPipeline.smove = block_pipeline_command(Redis.smove)
ClusterPipeline.sort = block_pipeline_command(Redis.sort)
ClusterPipeline.sunion = block_pipeline_command(Redis.sunion)
ClusterPipeline.sunionstore = block_pipeline_command(Redis.sunionstore)
ClusterPipeline.time = block_pipeline_command(Redis.time)


class PipelineCommand(object):
    """
    """

    def __init__(self, args, options=None, position=None):
        self.args = args
        if options is None:
            options = {}
        self.options = options
        self.position = position
        self.result = None
        self.node = None
        self.asking = False
        self.slot   = None


class NodeCommands(object):
    """
    """

    def __init__(self, parse_response, connection,
                 response_callbacks = None, use_multi = False ):
        """
        """
        self.parse_response = parse_response
        self.connection = connection
        self.response_callbacks = response_callbacks or dict()
        self.commands   = []
        self.iRsp       = []
        self.useMulti   = use_multi     # if true, use multi-exec where possible.

    def append(self, c):
        """
        """
        self.commands.append(c)

    def write(self):
        """
        Code borrowed from Redis so it can be fixed
        """
        connection = self.connection
        commands = self.commands

        # We are going to clobber the commands with the write, so go ahead
        # and ensure that nothing is sitting there from a previous run.
        for c in commands:
            c.result = None

        # build up all commands into a single request to increase network perf
        # send all the commands and catch connection and timeout errors.
        try:
            if  self.useMulti :
                cmds = self.addTransaction( commands )
            else :
                cmds = [ c.args for c in commands ]
            connection.send_packed_command( connection.pack_commands( cmds ) )
        except (ConnectionError, TimeoutError) as e:
            for c in commands:
                c.result = e

    def addTransaction( self, commands ):
        cmds    = []
        iRsp    = []                    # list of indices where the response should go

        nCmds   = len( commands )
        if nCmds        < 2:
            cmds        = [ c.args for    c in            commands  ]
            iRsp        = [ i      for i, c in enumerate( commands )]
            self.iRsp   = iRsp
            return cmds

        iRun    = []                    # list of indices in the run
        inArun  = False                 # in a run of commands for same slot
        for i  in range( nCmds - 1 ) :
            c1  = commands[ i      ]
            c2  = commands[ i  + 1 ]
          # starting a new run?
            if  not inArun and c1.slot == c2.slot :
                inArun  = True
                cmds.append( ( 'MULTI', ) )
                iRsp.append( None )     # should discard this rsp (will be "OK")
          # add the actual cmd
            inArun  = self.addCmd( cmds, c1, i,     iRsp, iRun, inArun, c1.slot == c2.slot )

      # remember to add the last commands
        inArun      = self.addCmd( cmds, c2, i + 1, iRsp, iRun, inArun, False )

        self.iRsp   = iRsp
        return cmds

    def addCmd( self, cmds, cmd, i, iRsp, iRun, inArun, sameSlot ):
        cmds.append( cmd.args )
        if  inArun:
            index = str( i     )        # in good case we should discard this rsp.
            iRsp.append( index )        # should discard this rsp (will be "QUEUED")
            iRun.append( i     )
        else:
            iRsp.append( i    )         # this response is for cmds[ i ]

      # ending a run?
        if  inArun  and not sameSlot :
            inArun  = False
            cmds.append( ( 'EXEC', ) )
            iRsp.append( iRun[ : ] )    # need [:] to create a shallow copy, because we clear iRun next.
            iRun.clear()                # must use clear(), since the caller owns iRun.
        return inArun

    def read(self):
        if  self.useMulti :
            self.readMultiExec()
        else :
            self.readPlain()

    def readPlain(self):
        """
        """
        connection = self.connection
        for c in self.commands:

            # if there is a result on this command, it means we ran into an exception
            # like a connection error. Trying to parse a response on a connection that
            # is no longer open will result in a connection error raised by redis-py.
            # but redis-py doesn't check in parse_response that the sock object is
            # still set and if you try to read from a closed connection, it will
            # result in an AttributeError because it will do a readline() call on None.
            # This can have all kinds of nasty side-effects.
            # Treating this case as a connection error is fine because it will dump
            # the connection object back into the pool and on the next write, it will
            # explicitly open the connection and all will be well.
            if c.result is None:
                try:
                    c.result = self.parse_response(connection, c.args[0], **c.options)
                except (ConnectionError, TimeoutError) as e:
                    for c in self.commands:
                        c.result = e
                    return
                except RedisError:
                    c.result = sys.exc_info()[1]

    def readMultiExec(self):
        """
        """
        # much of error handling copied from
        #     redis.client.Pipeline._execute_transaction
        connection  = self.connection
        commands    = self.commands
        for index  in self.iRsp :       # iRsp = list of indices into responses
            try:
                rsp = self.parse_response( connection, '_' )
            except (ConnectionError, TimeoutError) as e:
                for c in self.commands:
                    c.result = e
                return
            except RedisError:
                rsp = sys.exc_info()[ 1 ]

            if  index is None :
                # We should eat this response ('OK' from MULTI).
                # We used to eat 'QUEUED' for commands in multi-exec.  But it could
                # be moved or other errors.  In those case we want to record the
                # error to the commands.  This is handled in next section.
                # TODO: if the rsp to MULTI is not 'OK'?
                continue

            if  isinstance( index, str ) :
                # for commands that are part of a multi-exec transaction, we encode
                # the index as str (usually int).
                # If the result is "QUEUED" (good case), we can discard this result.
                # But if the result is an error, we need to store the error result.
                if  rsp == b'QUEUED' :
                    continue                 # good case, discard the placeholder rsp.
                index   = int( index )
                cmd     = commands[ index ]
                if  cmd . result is None :
                    cmd . result =  rsp      # error case, record the error.
                continue

            if  isinstance( index, int ) :
                # one rsp for one plain command
                indexList   = [ index ]
                rspList     = [ rsp   ]
            else :
                # list of rsp from EXEC (as in MULTI-EXEC)
                indexList   = index
                rspList     = rsp

            if  isinstance( rsp, ExecAbortError ) :
                # got exec abort error, eg. due to slot moved.
                # the errors were already recorded the the commands, no need
                # to do it again here in the EXEC step.
                continue

            if  len( rspList ) != len( indexList ) :
                self.connection.disconnect()
                raise ResponseError( "Wrong number of response items from "
                                     "pipeline execution" )

            for i, r in zip( indexList, rspList ) :
                cmd = commands[ i ]
                if  cmd.result is not None:
                    continue                            # already have the result

                # We have to run response callbacks manually
                if  not isinstance( r, Exception ):
                    args            = cmd.args
                    options         = cmd.options
                    command_name    = args[ 0 ]
                    if  command_name in self.response_callbacks :
                        r   = self.response_callbacks[ command_name ]( r, **options )

                cmd.result  = r
