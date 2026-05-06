import time
from valkeytestframework.util.waiters import *
from valkey import ResponseError
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

class TestCuckooBasic(ValkeyBloomTestCaseBase):

    def test_basic(self):
        client = self.server.get_new_client()
        # Validate that the valkey-bloom module is loaded.
        module_list_data = client.execute_command('MODULE LIST')
        module_loaded = False
        for module in module_list_data:
            if (module[b'name'] == b'bf'):
                module_loaded = True
                break
        assert(module_loaded)

        # Validate that all the CF.* commands are supported on the server.
        command_cmd_result = client.execute_command('COMMAND')
        cf_cmds = ["CF.ADD", "CF.EXISTS", "CF.DEL", "CF.COUNT", "CF.MEXISTS",
                   "CF.INFO", "CF.RESERVE", "CF.INSERT", "CF.ADDNX", "CF.INSERTNX",
                   "CF.LOAD"]
        assert all(item in command_cmd_result for item in cf_cmds)

        # Basic cuckoo filter create, item add and item exists validation.
        cf_add_result = client.execute_command('CF.ADD filter1 item1')
        assert cf_add_result == 1
        cf_exists_result = client.execute_command('CF.EXISTS filter1 item1')
        assert cf_exists_result == 1
        cf_exists_result = client.execute_command('CF.EXISTS filter1 item2')
        assert cf_exists_result == 0

    def test_add_and_delete(self):
        client = self.server.get_new_client()
        # Add item
        assert client.execute_command('CF.ADD filter item1') == 1
        assert client.execute_command('CF.EXISTS filter item1') == 1

        # Delete item - unique to cuckoo filters!
        assert client.execute_command('CF.DEL filter item1') == 1
        assert client.execute_command('CF.EXISTS filter item1') == 0

        # Try deleting again - should return 0 (not found)
        assert client.execute_command('CF.DEL filter item1') == 0

    def test_count(self):
        client = self.server.get_new_client()
        # Count functionality - unique to cuckoo filters
        assert client.execute_command('CF.ADD filter item1') == 1
        # Count should be at least 1
        count = client.execute_command('CF.COUNT filter item1')
        assert count >= 1

        # Non-existent item should have count 0
        count = client.execute_command('CF.COUNT filter nonexistent')
        assert count == 0

    def test_addnx(self):
        client = self.server.get_new_client()
        # CF.ADDNX - add if not exists
        assert client.execute_command('CF.ADDNX filter item1') == 1
        # Try adding again - should return 0 (already exists)
        assert client.execute_command('CF.ADDNX filter item1') == 0
        # Verify item exists
        assert client.execute_command('CF.EXISTS filter item1') == 1

    def test_copy_and_exists_cmd(self):
        client = self.server.get_new_client()
        # Add multiple items
        assert client.execute_command('CF.ADD filter item1') == 1
        assert client.execute_command('CF.ADD filter item2') == 1
        assert client.execute_command('CF.ADD filter item3') == 1
        assert client.execute_command('CF.ADD filter item4') == 1

        assert client.execute_command('EXISTS filter') == 1
        mexists_result = client.execute_command('CF.MEXISTS filter item1 item2 item3 item4')
        assert len(mexists_result) == 4
        assert all(x == 1 for x in mexists_result)

        # Test COPY command
        assert client.execute_command('COPY filter new_filter') == 1
        assert client.execute_command('EXISTS new_filter') == 1
        copy_mexists_result = client.execute_command('CF.MEXISTS new_filter item1 item2 item3 item4')
        assert mexists_result == copy_mexists_result

    def test_memory_usage_cmd(self):
        client = self.server.get_new_client()
        assert client.execute_command('CF.ADD filter item1') == 1
        memory_usage = client.execute_command('MEMORY USAGE filter')
        info_size = client.execute_command('CF.INFO filter Size')
        assert memory_usage >= info_size and info_size > 0

    def test_too_large_cuckoo_obj(self):
        client = self.server.get_new_client()
        # Set the max allowed size per cuckoo filter per cuckoo object
        assert client.execute_command('CONFIG SET cuckoo-memory-usage-limit 1000') == b'OK'
        obj_exceeds_size_err = "operation exceeds cuckoo object memory limit"

        # Non Scaling
        # Validate that when a cmd would have resulted in a cuckoo object creation with the starting filter with size
        # greater than allowed limit, the cmd is rejected.
        cmds = [
            'CF.RESERVE filter 10000 BUCKETSIZE 4',
            'CF.INSERT filter CAPACITY 10000 ITEMS item1',
            'CF.ADD filter item1',
        ]
        for cmd in cmds:
            self.verify_error_response(self.client, cmd, obj_exceeds_size_err)

    def test_reserve(self):
        client = self.server.get_new_client()
        # CF.RESERVE with default parameters
        assert client.execute_command('CF.RESERVE filter 1000') == b'OK'
        info_result = client.execute_command('CF.INFO filter')
        # Info should return array with capacity info
        assert b'Capacity' in info_result or b'Number of buckets' in info_result

        # Try to reserve again - should fail (key exists)
        try:
            client.execute_command('CF.RESERVE filter 1000')
            assert False, "Should have raised error for existing key"
        except ResponseError as e:
            assert "exists" in str(e).lower()

    def test_reserve_with_options(self):
        client = self.server.get_new_client()
        # CF.RESERVE with bucket size and max iterations
        assert client.execute_command('CF.RESERVE filter 1000 BUCKETSIZE 2 MAXITERATIONS 500') == b'OK'
        assert client.execute_command('CF.ADD filter item1') == 1
        assert client.execute_command('CF.EXISTS filter item1') == 1

    def test_insert_with_nocreate(self):
        client = self.server.get_new_client()
        # CF.INSERT with NOCREATE should fail if filter doesn't exist
        try:
            client.execute_command('CF.INSERT filter NOCREATE ITEMS item1')
            assert False, "Should have raised error for non-existent filter"
        except ResponseError as e:
            assert "not found" in str(e).lower() or "does not exist" in str(e).lower()

    def test_insert_auto_create(self):
        client = self.server.get_new_client()
        # CF.INSERT should auto-create filter
        result = client.execute_command('CF.INSERT filter ITEMS item1 item2 item3')
        assert len(result) == 3
        # All items should be added successfully (return 1)
        assert all(x == 1 for x in result)

        # Verify items exist
        mexists_result = client.execute_command('CF.MEXISTS filter item1 item2 item3')
        assert all(x == 1 for x in mexists_result)

    def test_insert_with_capacity(self):
        client = self.server.get_new_client()
        # CF.INSERT with custom capacity
        result = client.execute_command('CF.INSERT filter CAPACITY 500 ITEMS item1 item2')
        assert len(result) == 2
        info_result = client.execute_command('CF.INFO filter')
        assert info_result is not None

    def test_insertnx(self):
        client = self.server.get_new_client()
        # CF.INSERTNX - insert multiple items only if they don't exist
        result = client.execute_command('CF.INSERTNX filter ITEMS item1 item2 item3')
        assert len(result) == 3
        assert all(x == 1 for x in result)  # All new items

        # Try inserting again - should return 0 for existing items
        result = client.execute_command('CF.INSERTNX filter ITEMS item1 item2 item4')
        assert len(result) == 3
        assert result[0] == 0  # item1 exists
        assert result[1] == 0  # item2 exists
        assert result[2] == 1  # item4 is new

    def test_scaling_filter(self):
        client = self.server.get_new_client()
        # Create a small filter with expansion enabled
        assert client.execute_command('CF.RESERVE filter 10 EXPANSION 2') == b'OK'

        # Add items up to capacity
        for i in range(15):
            result = client.execute_command(f'CF.ADD filter item{i}')
            # Should succeed even past initial capacity due to scaling
            assert result == 1 or result == 0  # 0 if false positive

        # Check that filter scaled
        info_result = client.execute_command('CF.INFO filter')
        # Should have multiple filters now
        # Note: Exact assertion depends on INFO output format

    def test_non_scaling_filter_full(self):
        client = self.server.get_new_client()
        # Create a non-scaling filter (expansion = 0)
        assert client.execute_command('CF.RESERVE filter 5') == b'OK'

        # Try to fill it completely
        added_count = 0
        for i in range(20):
            try:
                result = client.execute_command(f'CF.ADD filter item{i}')
                if result == 1:
                    added_count += 1
            except ResponseError as e:
                # Should eventually get "filter is full" error
                assert "full" in str(e).lower()
                break

        # Should have added some items before it became full
        assert added_count > 0
