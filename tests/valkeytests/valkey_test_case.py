import subprocess
import time
import os
import pytest
import shutil
import re
from contextlib import contextmanager
from functools import wraps
from valkey import *
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

class ValkeyAction(Enum):
    AOF_REWRITE = 1

class ValkeyServerHandle(object):
    """Handle to a valkey server process"""
    
    DEFAULT_BIND_IP = "0.0.0.0"

    def __init__(self, bind_ip, port, port_tracker, server_path='valkey-server', cwd='.'):
        self.server = None
        self.client = None
        self.port = port
        self.bind_ip = bind_ip
        self.args = {}
        self.args["port"] = self.port
        self.args["logfile"] = "logfile_{}".format(port)
        self.args["dbfilename"] = "testrdb-{}.rdb".format(port)
        self.args["appenddirname"] = "aof-{}".format(port)
        self.cwd = cwd
        self.valkey_path = server_path

    @classmethod
    def create_from_server(self, server, db=0):
        logging.info(("Created regular client for port {}".format(server.port)))
        r = StrictValkey(host='localhost', port=server.port, db=db)
        return r
    
    def set_startup_args(self, args):
        self.args.update(args)

    def get_new_client(self):
        return self.create_from_server(self)

    def exit(self, cleanup=True, remove_nodes_conf=True):
        if self.client:
            try:
                self.client.shutdown('nosave')
            except:
                logging.warning("SHUTDOWN was unsuccessful")

            self.client = None

        if self.server:
            self._waitForExit()
            self.server = None

        if os.environ.get('SKIPLOGCLEAN') == None:
            if "logfile" in self.args and os.path.exists(os.path.join(self.cwd, self.args["logfile"])):
                os.remove(os.path.join(self.cwd, self.args["logfile"]))

            if cleanup and "appenddirname" in self.args and os.path.exists(os.path.join(self.cwd, self.args["appenddirname"])):
                shutil.rmtree(os.path.join(self.cwd, self.args["appenddirname"]))

        if cleanup and "dbfilename" in self.args and os.path.exists(os.path.join(self.cwd, self.args["dbfilename"])):
            try:
                os.remove(os.path.join(self.cwd, self.args["dbfilename"]))
            except OSError:
                os.rmdir(os.path.join(self.cwd, self.args["dbfilename"]))

        if remove_nodes_conf and "cluster-config-file" in self.args and os.path.exists(os.path.join(self.cwd, self.args["cluster-config-file"])):
            try:
                os.remove(os.path.join(self.cwd, self.args["cluster-config-file"]))
            except OSError:
                os.rmdir(os.path.join(self.cwd, self.args["cluster-config-file"]))

    def _waitForExit(self):
        try:
            self.wait_for_shutdown()
        except WaitTimeout:
            logging.warning("Server did not exit in time, killing...")
            if self.is_alive():
                # check server is still running before kill it.
                self.kill()
            try:
                self.wait_for_shutdown()
            except WaitTimeout:
                logging.error("Could not tear down server")
                assert False

    def pid(self):
        return self.server.pid

    def wait_for_shutdown(self):
        wait_for_ne(lambda: self.server.poll(), None, timeout=TEST_MAX_WAIT_TIME_SECONDS)

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
        wait_for_equal(lambda: self.client.info()["connected_slaves"], num_of_replicas, timeout=MAX_REPLICA_WAIT_TIME)

    def wait_for_ready_to_accept_connections(self):
        logfile = os.path.join(self.cwd, self.args['logfile'])
        strings = ['Ready to accept connections']
        wait_for_true(lambda: verify_any_of_strings_in_file(strings, logfile), timeout=TEST_MAX_WAIT_TIME_SECONDS)

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
        server_args.extend([self.valkey_path])
        for k, v in list(self.args.items()):
            server_args.append('--' + k.replace("_", "-"))
            args = str(v).split()
            for arg in args:
                server_args.append(arg)
        logging.info(server_args)

        # Provide some warnings to help debug failing tests
        if "cluster-config-file" in self.args and os.path.exists(os.path.join(self.cwd, self.args["cluster-config-file"])):
            logging.info(("cluster-config-file exists ({}) before startup for node with port {}".format(os.path.join(os.getcwd(), self.args["cluster-config-file"]), self.port)))

        if "dbfilename" in self.args and os.path.exists(os.path.join(self.cwd, self.args["dbfilename"])):
            logging.info("dbfilename exists before startup for node with port %d" % self.port)

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

    def _waitForPing(self, c):
        try:
            wait_for_true(lambda: c.ping(), timeout=MAX_PING_WAIT_TIME)
            return True
        except (ConnectionError, TimeoutError) as e:
            logging.error(e)
            return False

    def wait_for_key(self, key, value):
        if isinstance(value, str):
            value = value.encode()
        wait_for_equal(lambda: self.client.get(key), value, timeout=TEST_MAX_WAIT_TIME_SECONDS)

    def connect(self):
        c = self.create_from_server(self)
        try:
            self._waitForPing(c)
        except WaitTimeout:
             raise RuntimeError("Failed to connect or ping server")
        self.client = c

    def wait_for_save_done(self, client=None):
        """Wait for the save to complete, failing if it does not complete successfully in the timeout"""
        if client is None:
            client = self.client
        try:
            wait_for_ne(lambda: client.info()['rdb_bgsave_in_progress'], 1, timeout=TEST_MAX_WAIT_TIME_SECONDS)
        except WaitTimeout:
            raise RuntimeError("Save failed to complete in time")
        assert(client.info()['rdb_last_bgsave_status'] == 'ok')

    def wait_for_save_in_progress(self, client=None):
        if client is None:
            client = self.client
        wait_for_equal(lambda: client.info()['rdb_bgsave_in_progress'], 1, timeout=TEST_MAX_WAIT_TIME_SECONDS)

    def is_rdb_done_loading(self):
        rdb_load_log = "Done loading RDB"
        return self.verify_string_in_logfile(rdb_load_log) == True

    def num_replicas_online(self, client=None):
        if client is None:
            client = self.client
        count=0
        for k,v in client.info().items():
            if re.match('^slave[0-9]', k) and v['state'] == 'online':
                count += 1
        return count

    def get_default_client(self, client):
        if client is None:
            return self.client
        return client

    def num_keys(self, db=0, client = None):
        if client is None:
            client = self.client
        if f'db{db}'.format(db) in client.info('all').keys():
            return client.info('all')['db{}'.format(db)]['keys']
        return 0
    
    def is_primary_link_up(self, client=None):
        if client is None:
            client = self.client
        """Returns True if role is slave and master_link_status is up"""
        if client.info()['role'] == 'slave' and client.info()['master_link_status'] == 'up':
            return True
        return False

    def _action_success_flag(self, action, client):
        if action == ValkeyAction.AOF_REWRITE:
            return client.info()['aof_last_bgrewrite_status'] == 'ok'
        else:
            raise RuntimeError("{} not support".format(action))

    def wait_for_action_done(self, action, client=None):
        if client is None:
            client = self.client
        try:
            if action == ValkeyAction.AOF_REWRITE:
                wait_for_equal(lambda: client.info()['aof_rewrite_in_progress'], 1, timeout=TEST_MAX_WAIT_TIME_SECONDS)
            else:
                raise RuntimeError("{} not support".format(action))
        except WaitTimeout:
            raise RuntimeError("{} failed to complete in time".format(action))
        assert(self._action_success_flag(action, client))

class ValkeyTestCaseBase:
    testdir = "test-data"
    rdbdir = "rdbs"

    DEFAULT_BIND_IP = "0.0.0.0"

    @pytest.fixture(autouse=True)
    def port_tracker_fixture(self, resource_port_tracker):
        '''
        port_tracker_fixture using resource_port_tracker.
        '''
        # Inject port tracker
        logging.info("port tracker")
        self.args = {}
        self.port_tracker = resource_port_tracker

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

    def wait_for_logfile(self, filename, regex):
        wait_for_true(lambda: self.doesLogfileContain(filename, regex), timeout=TEST_MAX_WAIT_TIME_SECONDS)

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

    def waitForReplicaToSyncUp(self, server):
        wait_for_true(lambda: server.is_primary_link_up(), timeout=MAX_SYNC_WAIT)

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

    def get_bind_ip(self, multi_ip_mode=False):
        if multi_ip_mode:
            return self.ip_tracker.get_ip_address()
        return self.DEFAULT_BIND_IP

class ValkeyTestCase(ValkeyTestCaseBase):
    server_path = "valkey-server" # The default server build is assumed that valkey-server is set

    def common_setup(self):
        self.port = self.port_tracker.get_unused_port()
        self.ensureDirExists(self.testdir)
        self.server_list = []

    @pytest.fixture(autouse=True)
    def setup(self, port_tracker_fixture):
        self.common_setup()
        yield

    def get_valkey_handle(self):
        """Return valkey node handle. Allow child class to override the handle type"""
        return ValkeyServerHandle

    # Expose bind_ip parameter to caller to have more flexible
    def create_server(self, testdir, bind_ip=None, port=None, server_path=server_path, args=""):
        if not bind_ip:
            bind_ip = self.get_bind_ip()

        if not port:
            port = self.get_bind_port()
        valkey_server_handle = self.get_valkey_handle()
        self.server_path = server_path
        valkey_server = valkey_server_handle( bind_ip = bind_ip, port = port,
            port_tracker = self.port_tracker,
            cwd = testdir, server_path=server_path)
        self.server_list.append(valkey_server)
        valkey_server.args.update(args)
        valkey_cli = valkey_server.start()
        return valkey_server, valkey_cli

    def wait_for_all_replicas_online(self, n):
        wait_for_equal(lambda: self.server.num_replicas_online(), n, timeout=MAX_REPLICA_WAIT_TIME)

    def wait_for_replicas(self, n):
        self.server.wait_for_replicas(n)

    def teardown(self):
        if self.server:
            self.server.exit()
            self.server = None

class ValkeyReplica(ValkeyServerHandle):
    def __init__(self, primaryhost, primaryport, bind_ip, port, port_tracker,
                 testdir, server_path):
        super(ValkeyReplica, self).__init__(bind_ip, port, port_tracker,
                                             server_path, testdir)
        self.clients = []
        self.primaryhost = primaryhost
        self.primaryport = primaryport
        self.args["slaveof"] = self.primaryhost + " " + str(self.primaryport)

    def exit(self, remove_rdb=True, remove_nodes_conf=True):
        super(ValkeyReplica, self).exit(remove_rdb, remove_nodes_conf)
        del self.clients[:]

class ReplicationTestCase(ValkeyTestCase):
    num_replicas = 1

    def setup_replication(self, num_replicas = num_replicas):
        self.create_replicas(num_replicas)
        self.start_replicas()
        self.wait_for_all_replicas_online(self.num_replicas)
        self.wait_for_replicas(self.num_replicas)
        self.wait_for_primary_link_up_all_replicas()
        self.wait_for_all_replicas_online(self.num_replicas)
        for i in range(len(self.replicas)):
            self.waitForReplicaToSyncUp(self.replicas[i])

    def teardown(self):
        ValkeyTestCase.teardown(self)
        self.destroy_replicas()

    def _create_replica(self, primaryhost, primaryport, server_path):
        return ValkeyReplica(primaryhost, primaryport,
                            self.get_bind_ip(), self.get_bind_port(),
                            self.port_tracker, self.testdir, self.server_path)

    def create_replicas(self, num_replicas, primaryhost=None, primaryport=None,
                        connection_type='tcp', server_path=None):

        self.destroy_replicas()

        default_primaryhost = None
        default_port = None
        if connection_type == 'tcp':
            if hasattr(self.server, 'bind_ip'):
                default_primaryhost = self.server.bind_ip
            if hasattr(self.server, 'port'):
                default_port = self.server.port
        elif connection_type == 'unix':
            default_primaryhost = self.server.args["unixsocket"]
            default_port = 0    # Valkey treats the hostname as a unix socket path if the port is zero.
        else:
            raise ValueError("Invalid connection type %r, expected 'tcp' or 'unix'" % connection_type)

        if not primaryhost:
            primaryhost = default_primaryhost

        if not primaryport:
            primaryport = default_port

        self.num_replicas = num_replicas
        self.replicas = []
        for _ in range(self.num_replicas):
            replica = self._create_replica(primaryhost, primaryport, server_path)
            replica.set_startup_args(self.args)
            self.replicas.append(replica)

    def start_replicas(self, wait_for_ping=True):
        for i in range(self.num_replicas):
            self.replicas[i].start(wait_for_ping=wait_for_ping)

    def destroy_replicas(self):
        try:
            for i in range(self.num_replicas):
                self.replicas[i].exit()
        except AttributeError:
            logging.info("this test was skipped. Nothing to destroy")
            return
        self.num_replicas = 0
        del self.replicas[:]

    def wait_for_primary_link_up_all_replicas(self):
        for i in range(self.num_replicas):
            wait_for_true(lambda: self.replicas[i].is_primary_link_up(), timeout=MAX_SYNC_WAIT)

    def wait_for_value_propagate_to_replicas(self, key, value, db=0):
        for i in range(self.num_replicas):
            wait_for_equal(lambda: self.replicas[i].clients[db].get(key), value, timout=TEST_MAX_WAIT_TIME_SECONDS)

    def waitForReplicaOffsetToSyncUp(self, primary, replica):
        pinfo = primary.info()['master_repl_offset']
        wait_for_equal(lambda: replica.client.info()['slave_repl_offset'], pinfo.get_primary_repl_offset(), timeout=TEST_MAX_WAIT_TIME_SECONDS)
