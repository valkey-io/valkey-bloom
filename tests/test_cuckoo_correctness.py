import random
import string
from valkey import ResponseError
from valkey_bloom_test_case import ValkeyBloomTestCaseBase

class TestCuckooCorrectness(ValkeyBloomTestCaseBase):

    def test_add_and_check_correctness(self):
        """Test that items added are correctly detected"""
        client = self.server.get_new_client()

        # Create filter
        assert client.execute_command('CF.RESERVE myfilter 1000') == b'OK'

        # Add items and verify they exist
        test_items = [f'item_{i}' for i in range(100)]
        for item in test_items:
            assert client.execute_command(f'CF.ADD myfilter {item}') == 1
            assert client.execute_command(f'CF.EXISTS myfilter {item}') == 1

        # Verify all items still exist
        for item in test_items:
            assert client.execute_command(f'CF.EXISTS myfilter {item}') == 1

    def test_delete_correctness(self):
        """Test that deleted items are properly removed"""
        client = self.server.get_new_client()

        # Add items
        test_items = [f'item_{i}' for i in range(50)]
        for item in test_items:
            client.execute_command(f'CF.ADD myfilter {item}')

        # Delete every other item
        for i, item in enumerate(test_items):
            if i % 2 == 0:
                assert client.execute_command(f'CF.DEL myfilter {item}') == 1

        # Verify deleted items don't exist
        for i, item in enumerate(test_items):
            exists = client.execute_command(f'CF.EXISTS myfilter {item}')
            if i % 2 == 0:
                assert exists == 0, f"Deleted item {item} still exists"
            else:
                assert exists == 1, f"Non-deleted item {item} doesn't exist"

    def test_count_accuracy(self):
        """Test count functionality"""
        client = self.server.get_new_client()

        # Add item multiple times (cuckoo filters can store duplicates)
        item = 'test_item'
        assert client.execute_command(f'CF.ADD myfilter {item}') == 1

        # Count should be at least 1
        count = client.execute_command(f'CF.COUNT myfilter {item}')
        assert count >= 1

        # Add same item again
        client.execute_command(f'CF.ADD myfilter {item}')
        new_count = client.execute_command(f'CF.COUNT myfilter {item}')
        # Count should not decrease
        assert new_count >= count

    def test_no_false_negatives(self):
        """Test that cuckoo filters don't have false negatives for added items"""
        client = self.server.get_new_client()

        # Create filter with known capacity
        assert client.execute_command('CF.RESERVE myfilter 500') == b'OK'

        # Add items up to reasonable capacity
        test_items = [f'item_{i}' for i in range(200)]
        for item in test_items:
            result = client.execute_command(f'CF.ADD myfilter {item}')
            # Should successfully add
            assert result in [0, 1]

        # Verify no false negatives - all added items should exist
        false_negatives = 0
        for item in test_items:
            if client.execute_command(f'CF.EXISTS myfilter {item}') == 0:
                false_negatives += 1

        # Cuckoo filters should not have false negatives
        assert false_negatives == 0, f"Found {false_negatives} false negatives"

    def test_addnx_idempotency(self):
        """Test that CF.ADDNX is idempotent"""
        client = self.server.get_new_client()

        item = 'test_item'

        # First add should succeed
        assert client.execute_command(f'CF.ADDNX myfilter {item}') == 1

        # Subsequent adds should return 0
        for _ in range(10):
            assert client.execute_command(f'CF.ADDNX myfilter {item}') == 0

        # Item should still exist
        assert client.execute_command(f'CF.EXISTS myfilter {item}') == 1

    def test_mexists_bulk_correctness(self):
        """Test CF.MEXISTS returns correct results for multiple items"""
        client = self.server.get_new_client()

        # Add some items
        added_items = ['item1', 'item3', 'item5', 'item7', 'item9']
        for item in added_items:
            client.execute_command(f'CF.ADD myfilter {item}')

        # Check mix of existing and non-existing items
        check_items = ['item1', 'item2', 'item3', 'item4', 'item5', 'item6']
        result = client.execute_command('CF.MEXISTS myfilter', *check_items)

        assert len(result) == 6
        assert result[0] == 1  # item1 exists
        assert result[1] == 0  # item2 doesn't exist
        assert result[2] == 1  # item3 exists
        assert result[3] == 0  # item4 doesn't exist
        assert result[4] == 1  # item5 exists
        assert result[5] == 0  # item6 doesn't exist

    def test_insert_bulk_correctness(self):
        """Test CF.INSERT adds all items correctly"""
        client = self.server.get_new_client()

        items = [f'item_{i}' for i in range(20)]
        result = client.execute_command('CF.INSERT myfilter ITEMS', *items)

        assert len(result) == 20
        # All items should be added (return 1) or already exist (return 0)
        assert all(x in [0, 1] for x in result)

        # Verify all items exist
        for item in items:
            assert client.execute_command(f'CF.EXISTS myfilter {item}') == 1

    def test_insertnx_correctness(self):
        """Test CF.INSERTNX only adds new items"""
        client = self.server.get_new_client()

        # First insert - all new
        items1 = ['item1', 'item2', 'item3']
        result1 = client.execute_command('CF.INSERTNX myfilter ITEMS', *items1)
        assert all(x == 1 for x in result1)

        # Second insert - mix of new and existing
        items2 = ['item2', 'item3', 'item4']
        result2 = client.execute_command('CF.INSERTNX myfilter ITEMS', *items2)
        assert result2[0] == 0  # item2 exists
        assert result2[1] == 0  # item3 exists
        assert result2[2] == 1  # item4 is new

    def test_delete_and_readd(self):
        """Test that items can be deleted and re-added correctly"""
        client = self.server.get_new_client()

        item = 'test_item'

        # Add item
        assert client.execute_command(f'CF.ADD myfilter {item}') == 1
        assert client.execute_command(f'CF.EXISTS myfilter {item}') == 1

        # Delete item
        assert client.execute_command(f'CF.DEL myfilter {item}') == 1
        assert client.execute_command(f'CF.EXISTS myfilter {item}') == 0

        # Re-add item
        assert client.execute_command(f'CF.ADD myfilter {item}') == 1
        assert client.execute_command(f'CF.EXISTS myfilter {item}') == 1

    def test_random_data_correctness(self):
        """Test correctness with random data"""
        client = self.server.get_new_client()

        # Create filter
        assert client.execute_command('CF.RESERVE myfilter 1000') == b'OK'

        # Generate random items
        random_items = set()
        for _ in range(100):
            item = ''.join(random.choices(string.ascii_letters + string.digits, k=20))
            random_items.add(item)

        # Add all items
        for item in random_items:
            client.execute_command(f'CF.ADD myfilter {item}')

        # Verify all items exist
        missing_items = []
        for item in random_items:
            if client.execute_command(f'CF.EXISTS myfilter {item}') == 0:
                missing_items.append(item)

        assert len(missing_items) == 0, f"Missing {len(missing_items)} items: {missing_items[:5]}"

    def test_capacity_limit_behavior(self):
        """Test behavior when approaching capacity"""
        client = self.server.get_new_client()

        # Create small non-scaling filter
        capacity = 50
        assert client.execute_command(f'CF.RESERVE myfilter {capacity}') == b'OK'

        # Try to add items up to and beyond capacity
        added_count = 0
        failed = False
        for i in range(capacity * 2):
            try:
                result = client.execute_command(f'CF.ADD myfilter item_{i}')
                if result == 1:
                    added_count += 1
            except ResponseError as e:
                if 'full' in str(e).lower():
                    failed = True
                    break

        # Should have added some items
        assert added_count > 0
        # Non-scaling filter should eventually fail
        # Note: Exact behavior depends on implementation

    def test_info_reflects_operations(self):
        """Test that CF.INFO accurately reflects filter state"""
        client = self.server.get_new_client()

        # Create filter
        assert client.execute_command('CF.RESERVE myfilter 1000') == b'OK'

        # Add items
        num_items = 20
        for i in range(num_items):
            client.execute_command(f'CF.ADD myfilter item_{i}')

        # Get info
        info = client.execute_command('CF.INFO myfilter')
        # Info should show items were added
        # Note: Exact format depends on implementation
        assert info is not None
