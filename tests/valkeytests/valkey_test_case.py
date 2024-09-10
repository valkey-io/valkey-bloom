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
from enum import Enum

class ValkeyServerVersion(Enum):
    LATEST = ".build/binaries/unstable/valkey-server"
    V7_2_6 = ".build/binaries/7.2.6/valkey-server"
    V7_2_5 = ".build/binaries/7.2.5/valkey-server"

MAX_PING_TRIES = 60

# The maximum wait time for operations in the tests
TEST_MAX_WAIT_TIME_SECONDS = 90

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

class wait(object):
    """Decorator to wait a configurable amount of time for a condition to become true."""

    def __init__(self, sleep = 1, max_time_to_wait=TEST_MAX_WAIT_TIME_SECONDS):
        self.max_time_to_wait = max_time_to_wait
        self.sleep = sleep

    def __call__(self, func):
        @wraps(func)
        def func_wrapper(*args, **kwargs):
            for _ in range(self.max_time_to_wait):
                if func(*args, **kwargs):
                    return True
                time.sleep(self.sleep)
            return False
        return func_wrapper

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

    def was_save_successful(self):
        return self.info['rdb_last_bgsave_status'] == 'ok'

    def used_memory(self):
        return self.info['used_memory']


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


    def __init__(self, port, version: ValkeyServerVersion, cwd="."):
        self.server = None
        self.client = None
        self.port = port
        self.args = {}
        self.args["port"] = self.port
        self.args["logfile"] = "logfile_{}".format(port)
        self.args["dbfilename"] = "testrdb-{}.rdb".format(port)
        self.cwd = cwd
        self.valkey_path = version.value

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
        if not self._waitForServerPoll():
            print("Server did not exit in time, killing...")
            self.server.kill()
            if not self._waitForServerPoll():
                print("Could not tear down server")
                assert(False)

    def pid(self):
        return self.server.pid

    @wait(1, 5)
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

    @wait(1, 60) # wait upto 30 sec checking every sec
    def wait_for_ready_to_accept_connections(self):
        logfile = os.path.join(self.cwd, self.args['logfile'])
        stings = ['Ready to accept connections']
        return verify_any_of_strings_in_file(stings, logfile)

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

    def start(self, connect_client=True):
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
            if not self.wait_for_ready_to_accept_connections():
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
        self.start(connect_client)

    def is_alive(self):
        try:
            self.client.ping()
            return True
        except:
            return False

    @wait(1, MAX_PING_TRIES)
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
        if not self._waitForPing(c):
            raise RuntimeError("Failed to connect or ping server")
        self.client = c

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
        assert(self._wait_for_save(client))
        assert(client.info_obj().was_save_successful())

    def wait_for_save_in_progress(self):
        assert(self._wait_for_save_in_progress())

    @wait()
    def _wait_for_save_in_progress(self):
        return self.client.info_obj().is_save_in_progress()

class ValkeyTestCaseBase:
    testdir = "test-data"
    rdbdir = "rdbs"

    def get_custom_args(self):
        return {}

    @pytest.fixture(autouse=True)
    def port_tracker_fixture(self, resource_port_tracker):
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
    def _wait_for_logfile(self, filename, regex):
        return self.doesLogfileContain(filename, regex)

    def wait_for_logfile(self, filename, regex):
        assert(self._wait_for_logfile(filename, regex))

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

class ValkeyTestCase(ValkeyTestCaseBase):
    num_dbs = 5
    num_keys = 100
    rdb_size = 168231
    repl_save_info_size = 83 # Bytes used for saving replication info in RDB aux fields
    diskless_overhead = 87 # RDB overhead is 2 x 40 byte EOF marker + 7 characters ("$EOF:" + "\r\n") for the beginning of the EOF marker
    version = ValkeyServerVersion.LATEST

    def set_server_version(self, new_version):
        self.version = new_version

    def common_setup(self):
        self.maxmemory = "500MB"
        self.port = self.port_tracker.get_unused_port()
        self.ensureDirExists(self.testdir)

    def setup(self):
        self.common_setup()
        args = self._get_valkey_args()
        self.server = ValkeyServerHandle(self.port, self.version, self.testdir)
        self.server.set_startup_args(args)
        print("startup args are: ", args)

        self.client = self.server.start()
        self.clients = []
        for db in range(self.num_dbs):
            self.clients.append(ValkeyClient.create_from_server(self.server, db))

    def teardown(self):
        if self.server:
            self.server.exit()
            self.server = None

    def set_small_amount_of_keys(self):
        for i in range(self.num_keys):
            self.clients[0].set('key_{}'.format(i), i)
