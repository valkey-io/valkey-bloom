import subprocess
import time
import random
import os
import pytest
import re
import struct
import threading
import io
import socket
from contextlib import contextmanager
from functools import wraps
from valkey import *
from valkey.client import Pipeline
from util.waiters import *

from enum import Enum

MAX_PING_TRIES = 60

# The maximum wait time for operations in the tests
TEST_MAX_WAIT_TIME_SECONDS = 90
MAX_REPLICA_WAIT_TIME = 120
MAX_SYNC_WAIT = 90
MAX_PING_WAIT_TIME = 30

# Return true if the specified string is present in the provided file
def verify_string_in_file(string, filename):
    if not os.path.exists(filename):
        return False

    with open(filename) as f:
        for line in f:
            if string in line:
                return True
    return False

# Return true if the any of the strings is present in the provided file
def verify_any_of_strings_in_file(strings, filename):
    if not os.path.exists(filename):
        return False

    with open(filename, encoding="latin-1") as f:
        for line in f:
            for string in strings:
                if string in line:
                    return True
    return False

class ExpectException(Exception):
    def __init__(self, lhs, op, rhs):
        self.lhs = lhs
        self.op = op
        self.rhs = rhs

def expect(lhs, op, rhs):
    if not op(lhs, rhs):
        raise ExpectException(lhs, op, rhs)


@wait()
def wait_for_true(expr):
    return expr

class ValkeyInfo:
    """Contains information about a point in time of Valkey"""
    def __init__(self, info):
        self.info = info

    def is_save_in_progress(self):
        """Return True if there is a save in progress."""
        return self.info['rdb_bgsave_in_progress'] == 1

    def num_keys(self, db=0):
        if 'db{}'.format(db) in self.info:
            return self.info['db{}'.format(db)]['keys']
        return 0

    def get_master_repl_offset(self):
        return self.info['master_repl_offset']

    def get_master_replid(self):
        return self.info['master_replid']

    def get_replica_repl_offset(self):
        return self.info['slave_repl_offset']

    def is_master_link_up(self):
        """Returns True if role is slave and master_link_status is up"""
        if self.info['role'] == 'slave' and self.info['master_link_status'] == 'up':
            return True
        return False

    def num_replicas(self):
        return self.info['connected_slaves']

    def num_replicas_online(self):
        count=0
        for k,v in self.info.items():
            if re.match('^slave[0-9]', k) and v['state'] == 'online':
                count += 1
        return count

    def was_save_successful(self):
        return self.info['rdb_last_bgsave_status'] == 'ok'

    def used_memory(self):
        return self.info['used_memory']

    def maxmemory(self):
        return self.info['maxmemory']

    def maxmemory_policy(self):
        return self.info['maxmemory_policy']

    def uptime_in_secs(self):
        return self.info['uptime_in_seconds']

# An extension of the StrictValkey client
# that supports additional Valkey functionality
class ValkeyClient(StrictValkey):
    def set_password(self, pw):
        """
        Set the password for the server's connection.  Must be called after requirepass has been applied.
        """
        self.connection_pool.reset()
        self.connection_pool.connection_kwargs.update({'password': pw})

    def get_connection(self):
        """
        Obtain a raw connection from the client's connection pool.  Callers must ensure that they
        return the connection via release_connection().
        """
        return self.connection_pool.get_connection(None)

    def release_connection(self, connection):
        """
        Release a raw connection back into the pool.
        """
        self.connection_pool.release(connection)

    @classmethod
    def create_from_server(self, server, db=0):
        print(("Created regular client for port {}".format(server.port)))
        r = ValkeyClient(host='localhost', port=server.port, db=db)
        return r

    # Add a flag to shutdown so that we can avoid a save
    def shutdown(self, flag=None):
        """Shutdown the server.

        Args:
            flag: an optional argument to indicate whether a snapshot should be taken
        """
        try:
            if flag:
                self.execute_command('SHUTDOWN', flag)
            else:
                self.shutdown()
        except ConnectionError:
            # a ConnectionError here is expected
            return
        raise ValkeyError("SHUTDOWN seems to have failed.")

    def bgsave(self, type=None):
        """Perform a background save.

        Args:
            type: an optional argument to indicate the type of snapshot.
        """
        if type is None:
            return self.execute_command('BGSAVE')
        else:
            return self.execute_command('BGSAVE', type)

    def restore(self, name, ttl, value, replace=False):
        """Create a key using the provided serialized value, previously obtained
        using DUMP.
        """
        return self.execute_command('RESTORE', name, ttl, value, 'REPLACE' if replace else "")

    def info_obj(self):
        """Return a ValkeyInfo object for the current client info"""
        return ValkeyInfo(self.info('all'))

    def bitfield(self, name, *values):
        """
        Executes specified subcommands to the specified key identified by
        the ``name`` argument. We are not doing any syntax validation of
        the specified subcommands idenfied by ``values``
        """
        return self.execute_command('BITFIELD', name, *values)

    def debug_digest(self):
        return self.execute_command('DEBUG', 'DIGEST')

class ValkeyServerHandle(object):
    """Handle to a valkey server process"""
    
    DEFAULT_BIND_IP = "0.0.0.0"

    def __init__(self, bind_ip, port, port_tracker, server_path, cwd='.', server_id=0):
        self.server = None
        self.client = None
        self.port = port
        self.bind_ip = bind_ip
        self.server_id = server_id
        self.args = {}
        self.args["port"] = self.port
        self.args["logfile"] = "logfile_{}".format(port)
        self.args["dbfilename"] = "testrdb-{}.rdb".format(port)
        self.cwd = cwd
        self.valkey_path = server_path

    def set_startup_args(self, args):
        self.args.update(args)

    def get_new_client(self):
        return ValkeyClient.create_from_server(self)

    def exit(self, remove_rdb=True, remove_nodes_conf=True):
        if self.client:
            try:
                self.client.shutdown('nosave')
            except:
                print("SHUTDOWN was unsuccessful")

            self.client = None

        if self.server:
            self._waitForExit()
            self.server = None

        if os.environ.get('SKIPLOGCLEAN') == None:
            if "logfile" in self.args and os.path.exists(os.path.join(self.cwd, self.args["logfile"])):
                os.remove(os.path.join(self.cwd, self.args["logfile"]))

        if remove_rdb and "dbfilename" in self.args and os.path.exists(os.path.join(self.cwd, self.args["dbfilename"])):
            try:
                os.remove(os.path.join(self.cwd, self.args["dbfilename"]))
            except OSError:
                os.rmdir(os.path.join(self.cwd, self.args["dbfilename"]))

        if remove_nodes_conf and "cluster-config-file" in self.args and os.path.exists(os.path.join(self.cwd, self.args["cluster-config-file"])):
            try:
                os.remove(os.path.join(self.cwd, self.args["cluster-config-file"]))
            except OSError:
                os.rmdir(os.path.join(self.cwd, self.args["cluster-config-file"]))

    @wait()
    def _waitForServerPoll(self):
        return self.server.poll() != None

    def _waitForExit(self):
        try:
            self._waitForServerPoll()
        except WaitTimeout:
            print("Server did not exit in time, killing...")
            if self.is_alive():
                # check server is still running before kill it.
                self.kill()
            try:
                self._waitForServerPoll()
            except WaitTimeout:
                print("Could not tear down server")
                assert False

    def pid(self):
        return self.server.pid

    @wait(timeout = 5)
    def is_down(self):
        return self.server.poll() != None

    def children_pids(self):
        process = subprocess.Popen("ps --no-headers -o pid --ppid %s" % self.pid(),
                             shell=True,
                             stdout=subprocess.PIPE,)
        children = list()
        for line in process.communicate()[0].split('\n'):
            line = line.strip()
            if line != "":
                children.append(line)
        return children

    def wait_for_replicas(self, num_of_replicas):
        wait_for_equal(lambda: self.client.info_obj().num_replicas(), num_of_replicas, timeout=MAX_REPLICA_WAIT_TIME)

    @wait(timeout = 60) # wait upto 30 sec checking every sec
    def wait_for_ready_to_accept_connections(self):
        logfile = os.path.join(self.cwd, self.args['logfile'])
        strings = ['Ready to accept connections']
        return verify_any_of_strings_in_file(strings, logfile)

    def verify_string_in_logfile(self, string):
        logfile = os.path.join(self.cwd, self.args['logfile'])
        return verify_string_in_file(string, logfile)

    @contextmanager
    def expect_crash(self, valkey_test, timeout=30, period=0.1):
        valkey_test.crash_expected = True
        try:
            yield
        except Exception:
            pass
        finally:
            start_time = time.time()
            while self.is_alive() and time.time() < start_time + timeout:
                time.sleep(period)
            if self.is_alive():
                pytest.fail(f"Valkey server did not crash as expected within {time.time() - start_time} seconds. ")

    def start(self, wait_for_ping=True, connect_client=True):
        if self.server:
            raise RuntimeError("Server already started")
        server_args = []
        server_args.extend([('%s/../' + self.valkey_path) % os.path.dirname(os.path.realpath(__file__))])
        for k, v in list(self.args.items()):
            server_args.append('--' + k.replace("_", "-"))
            args = str(v).split()
            for arg in args:
                server_args.append(arg)
        print(server_args)

        # Provide some warnings to help debug failing tests
        if "cluster-config-file" in self.args and os.path.exists(os.path.join(self.cwd, self.args["cluster-config-file"])):
            print(("cluster-config-file exists ({}) before startup for node with port {}".format(os.path.join(os.getcwd(), self.args["cluster-config-file"]), self.port)))

        if "dbfilename" in self.args and os.path.exists(os.path.join(self.cwd, self.args["dbfilename"])):
            print("dbfilename exists before startup for node with port %d" % self.port)

        self.server = subprocess.Popen(server_args, cwd=self.cwd)
        if connect_client:
            try:
                self.wait_for_ready_to_accept_connections()
            except WaitTimeout:
                raise RuntimeError("Valkey server is not Ready to accept connections")
            try:
                self.connect()
            except:
                # It's possible that the port was not fully released, so try again
                self.server.kill()
                time.sleep(1)
                self.server = subprocess.Popen(server_args, cwd=self.cwd)
                self.connect()

        return self.client

    def restart(self, remove_rdb=True, remove_nodes_conf=True, connect_client=True):
        self.exit(remove_rdb, remove_nodes_conf)
        self.start(connect_client=connect_client)

    def is_alive(self):
        try:
            self.client.ping()
            return True
        except:
            return False

    @wait(timeout = MAX_PING_WAIT_TIME)
    def _waitForPing(self, c):
        try:
            return c.ping()
        except (ConnectionError, TimeoutError) as e:
            print(e)
            return False

    @wait()
    def wait_for_key(self, key, value):
        if isinstance(value, str):
            value = value.encode()
        return self.client.get(key)== value

    def connect(self):
        c = ValkeyClient.create_from_server(self)
        try:
            self._waitForPing(c)
        except WaitTimeout:
             raise RuntimeError("Failed to connect or ping server")
        self.client = c

    def wait_for_all_replicas_online(self, num_of_replicas):
        """Wait for n replicas to show online"""
        wait_for_equal(lambda: self.client.info_obj().num_replicas_online(), num_of_replicas, timeout=MAX_REPLICA_WAIT_TIME)

    @wait()
    def _wait_for_save(self, client=None):
        """Wait the default number of seconds for the save to finish"""
        if client is None:
            client = self.client
        if client.info_obj().is_save_in_progress():
            return False
        return True

    def wait_for_save_done(self, client=None):
        """Wait for the save to complete, failing if it does not complete successfully in the timeout"""
        if client is None:
            client = self.client
        try:
            self._wait_for_save(client)
        except WaitTimeout:
            raise RuntimeError("Save failed to complete in time")
        assert(client.info_obj().was_save_successful())

    def wait_for_save_in_progress(self):
        assert(self._wait_for_save_in_progress())

    @wait()
    def _wait_for_save_in_progress(self):
        return self.client.info_obj().is_save_in_progress()

    def is_rdb_done_loading(self):
        rdb_load_log = "Done loading RDB"
        return self.verify_string_in_logfile(rdb_load_log) == True

class ValkeyTestCaseBase:
    testdir = "test-data"
    rdbdir = "rdbs"

    DEFAULT_BIND_IP = "0.0.0.0"

    def get_custom_args(self):
        return {}

    @pytest.fixture(autouse=True)
    def port_tracker_fixture(self, resource_port_tracker):
        '''
        port_tracker_fixture using resource_port_tracker.
        '''
        # Inject port tracker
        print ("port tracker")
        self.args = {}
        self.port_tracker = resource_port_tracker

    def _get_valkey_args(self):
        self.args.update({"maxmemory":self.maxmemory, "maxmemory-policy":"allkeys-random", "activerehashing":"yes", "databases": self.num_dbs, "repl-diskless-sync": "yes", "save": ""})
        self.args.update(self.get_custom_args())
        return self.args

    def ensureDirExists(self, dir):
        if not os.path.isdir(self.testdir):
            try:
                os.mkdir(self.testdir)
            except:
                assert(os.path.isdir(self.testdir)) # If tests have conflicted with each other check again

    def findLogfileLine(self, filename, regex):
        try:
            logfile = open(filename, "r")
            for line in logfile:
                match = re.search(regex, line)
                if match:
                    return match
            return None
        except:
            return None

    def doesLogfileContain(self, filename, regex):
        return self.findLogfileLine(filename, regex) != None

    @wait()
    def wait_for_logfile(self, filename, regex):
        return self.doesLogfileContain(filename, regex)

    def check_all_keys_in_valkey(self, node, dictionary):
        """ Check that all the keys in Valkey matches that in the dictionary """
        num_keys_in_valkey = 0
        for key in node.client.scan_iter():
            if dictionary.keys():
                if (isinstance(list(dictionary.keys())[0], str) and
                    isinstance(key, bytes)):
                    key = key.decode()

            assert(node.client.get(key) == str.encode(dictionary[key]))
            num_keys_in_valkey += 1
        return num_keys_in_valkey

    @wait(timeout=MAX_SYNC_WAIT)
    def waitForReplicaToSyncUpByClient(self, client):
        return client.info_obj().is_master_link_up()

    def waitForReplicaToSyncUp(self, server):
        return self.waitForReplicaToSyncUpByClient(server.client)

    # Wait until a client in the Valkey is executing a command
    # Used to ensure that a thread running a blocking command has started
    # Return True if the command is running, False if timeout
    def wait_until_command(self, server, cmd):
        wait_seconds = 0
        while wait_seconds < TEST_MAX_WAIT_TIME_SECONDS:
            for client in server.client.client_list():
                if client['cmd'] == cmd:
                    return True
            time.sleep(1)
            wait_seconds += 1
        return False

    def get_bind_port(self):
        return self.port_tracker.get_unused_port()

    @pytest.fixture(autouse=True)
    def server_id_fixture(self):
        self.server_id = 0
    
    def get_bind_ip(self, multi_ip_mode=False):
        if multi_ip_mode:
            return self.ip_tracker.get_ip_address()
        return self.DEFAULT_BIND_IP

class ValkeyTestCase(ValkeyTestCaseBase):
    num_dbs = 5
    num_keys = 100
    rdb_size = 168231
    repl_save_info_size = 83 # Bytes used for saving replication info in RDB aux fields
    diskless_overhead = 87 # RDB overhead is 2 x 40 byte EOF marker + 7 characters ("$EOF:" + "\r\n") for the beginning of the EOF marker
    server_path = ".build/binaries/unstable/valkey-server" #Unstable is the default server build

    def set_server_version(self, new_version):
        self.server_path = f".build/binaries/{new_version}/valkey-server"

    def common_setup(self):
        self.maxmemory = "500MB"
        self.port = self.port_tracker.get_unused_port()
        self.ensureDirExists(self.testdir)
        self.server_list = []


    def setup(self):
        self.common_setup()
        args = self._get_valkey_args()
        self.server = self.create_server(testdir = self.testdir,  server_path=self.server_path)
        self.server.set_startup_args(args)
        print("startup args are: ", args)

        self.client = self.server.start()
        self.clients = []
        for db in range(self.num_dbs):
            self.clients.append(ValkeyClient.create_from_server(self.server, db))

    def get_valkey_handle(self):
        """Return valkey node handle. Allow child class to override the handle type"""
        return ValkeyServerHandle

    # Expose bind_ip parameter to caller to have more flexible
    def create_server(self, testdir, bind_ip=None, port=None, server_path=server_path):
        if not bind_ip:
            bind_ip = self.get_bind_ip()

        if not port:
            port = self.get_bind_port()
            
        self.server_id += 1
        valkey_server_handle = self.get_valkey_handle()
        valkey_server = valkey_server_handle( bind_ip = bind_ip, port = port,
            port_tracker = self.port_tracker,
            cwd = testdir, server_id = self.server_id, server_path=server_path)
        self.server_list.append(valkey_server)
        return valkey_server
    
    def wait_for_all_replicas_online(self, n):
        self.server.wait_for_all_replicas_online(n)

    def wait_for_replicas(self, n):
        self.server.wait_for_replicas(n)

    def teardown(self):
        if self.server:
            self.server.exit()
            self.server = None

    def set_small_amount_of_keys(self):
        for i in range(self.num_keys):
            self.clients[0].set('key_{}'.format(i), i)

class ValkeyReplica(ValkeyServerHandle):
    def __init__(self, masterhost, masterport, bind_ip, port, port_tracker,
                 testdir, server_id, server_path):
        super(ValkeyReplica, self).__init__(bind_ip, port, port_tracker,
                                             server_path, testdir, server_id)
        self.clients = []
        self.masterhost = masterhost
        self.masterport = masterport
        self.args["slaveof"] = self.masterhost + " " + str(self.masterport)

    def exit(self, remove_rdb=True, remove_nodes_conf=True):
        super(ValkeyReplica, self).exit(remove_rdb, remove_nodes_conf)
        del self.clients[:]
    
    def create_client_for_dbs(self, num_dbs):
        for db in range(num_dbs):
            self.clients.append(ValkeyClient.create_from_server(self, db))
        return self.clients

class ReplicationTestCase(ValkeyTestCase):
    num_replicas = 1

    def setup_replication(self, num_replicas = num_replicas):
        super(ReplicationTestCase, self).setup()
        self.create_replicas(num_replicas)
        self.start_replicas()
        for i in range(len(self.replicas)):
            self.replicas[i].set_startup_args(self.get_custom_args())
        self.wait_for_all_replicas_online(self.num_replicas)
        self.wait_for_replicas(self.num_replicas)
        self.wait_for_master_link_up_all_replicas()
        self.wait_for_all_replicas_online(self.num_replicas)
        for i in range(len(self.replicas)):
            self.waitForReplicaToSyncUp(self.replicas[i])

    def teardown(self):
        ValkeyTestCase.teardown(self)
        self.destroy_replicas()

    def _create_replica(self, masterhost, masterport, server_path):
        self.server_id += 1
        return ValkeyReplica(masterhost, masterport,
                            self.get_bind_ip(), self.get_bind_port(),
                            self.port_tracker, self.testdir, self.server_id, self.server_path)

    def create_replicas(self, num_replicas, masterhost=None, masterport=None,
                        connection_type='tcp', server_path=None):

        self.destroy_replicas()

        default_masterhost = None
        default_port = None
        if connection_type == 'tcp':
            if hasattr(self.server, 'bind_ip'):
                default_masterhost = self.server.bind_ip
            if hasattr(self.server, 'port'):
                default_port = self.server.port
        elif connection_type == 'unix':
            default_masterhost = self.server.args["unixsocket"]
            default_port = 0    # Valkey treats the hostname as a unix socket path if the port is zero.
        else:
            raise ValueError("Invalid connection type %r, expected 'tcp' or 'unix'" % connection_type)

        if not masterhost:
            masterhost = default_masterhost

        if not masterport:
            masterport = default_port

        self.num_replicas = num_replicas
        self.replicas = []
        for _ in range(self.num_replicas):
            replica = self._create_replica(masterhost, masterport, server_path)
            replica.set_startup_args(self._get_valkey_args())
            self.replicas.append(replica)

    def start_replicas(self, wait_for_ping=True):
        for i in range(self.num_replicas):
            self.replicas[i].start(wait_for_ping=wait_for_ping)
            self.replicas[i].create_client_for_dbs(self.num_dbs)

    def destroy_replicas(self):
        try:
            for i in range(self.num_replicas):
                self.replicas[i].exit()
        except AttributeError:
            print("this test was skipped. Nothing to destroy")
            return
        self.num_replicas = 0
        del self.replicas[:]

    @wait()
    def wait_for_master_link_up_all_replicas(self):
        for i in range(self.num_replicas):
            if self.replicas[i].client.info_obj().is_master_link_up() == False:
                return False
        return True

    @wait()
    def wait_for_value_propagate_to_replicas(self, key, value, db=0):
        for i in range(self.num_replicas):
            if str(value) != self.replicas[i].clients[db].get(key):
                return False
        return True

    @wait()
    def waitForReplicaOffsetToSyncUp(self, master, replica):
        minfo = master.client.info_obj()
        rinfo = replica.client.info_obj()
        if minfo.get_master_repl_offset() == rinfo.get_replica_repl_offset():
            return True

        print("MASTER: master_repl_offset({0}), REPLICA: master_repl_offset({1}), slave_repl_offset({2}), slave_read_repl_offset({3})".format(
                minfo.info['master_repl_offset'],
                rinfo.info['master_repl_offset'],
                rinfo.info['slave_repl_offset'],
                rinfo.info['slave_read_repl_offset']))
        return False
