import pytest
from valkey import ResponseError
from valkey_bloom_test_case import ValkeyBloomTestCaseBase

class TestCuckooCommand(ValkeyBloomTestCaseBase):

    def test_cf_add_command(self):
        """Test CF.ADD command"""
        client = self.server.get_new_client()

        # Test basic add
        assert client.execute_command('CF.ADD myfilter item1') == 1
        assert client.execute_command('CF.EXISTS myfilter item1') == 1

        # Add duplicate (cuckoo filters can handle duplicates)
        result = client.execute_command('CF.ADD myfilter item1')
        assert result in [0, 1]  # May return 0 if already exists

        # Wrong number of arguments
        with pytest.raises(ResponseError):
            client.execute_command('CF.ADD')
        with pytest.raises(ResponseError):
            client.execute_command('CF.ADD myfilter')

    def test_cf_addnx_command(self):
        """Test CF.ADDNX command"""
        client = self.server.get_new_client()

        # Add new item
        assert client.execute_command('CF.ADDNX myfilter item1') == 1
        # Try adding existing item
        assert client.execute_command('CF.ADDNX myfilter item1') == 0

        # Wrong number of arguments
        with pytest.raises(ResponseError):
            client.execute_command('CF.ADDNX')
        with pytest.raises(ResponseError):
            client.execute_command('CF.ADDNX myfilter')

    def test_cf_del_command(self):
        """Test CF.DEL command - unique to cuckoo filters!"""
        client = self.server.get_new_client()

        # Add and delete
        assert client.execute_command('CF.ADD myfilter item1') == 1
        assert client.execute_command('CF.DEL myfilter item1') == 1
        assert client.execute_command('CF.EXISTS myfilter item1') == 0

        # Delete non-existent item
        assert client.execute_command('CF.DEL myfilter item2') == 0

        # Delete from non-existent filter
        try:
            client.execute_command('CF.DEL nonexistent item1')
            assert False, "Should have raised error"
        except ResponseError:
            pass

        # Wrong number of arguments
        with pytest.raises(ResponseError):
            client.execute_command('CF.DEL')
        with pytest.raises(ResponseError):
            client.execute_command('CF.DEL myfilter')

    def test_cf_count_command(self):
        """Test CF.COUNT command - unique to cuckoo filters!"""
        client = self.server.get_new_client()

        # Count in new filter (should be 0)
        count = client.execute_command('CF.COUNT nonexistent item1')
        assert count == 0 or count is None  # Might return 0 or error for non-existent

        # Add item and count
        assert client.execute_command('CF.ADD myfilter item1') == 1
        count = client.execute_command('CF.COUNT myfilter item1')
        assert count >= 1  # Should be at least 1

        # Count non-existent item in existing filter
        count = client.execute_command('CF.COUNT myfilter item2')
        assert count == 0

        # Wrong number of arguments
        with pytest.raises(ResponseError):
            client.execute_command('CF.COUNT')
        with pytest.raises(ResponseError):
            client.execute_command('CF.COUNT myfilter')

    def test_cf_exists_command(self):
        """Test CF.EXISTS command"""
        client = self.server.get_new_client()

        # Check non-existent item
        result = client.execute_command('CF.EXISTS myfilter item1')
        assert result == 0

        # Add and check
        assert client.execute_command('CF.ADD myfilter item1') == 1
        assert client.execute_command('CF.EXISTS myfilter item1') == 1

        # Wrong number of arguments
        with pytest.raises(ResponseError):
            client.execute_command('CF.EXISTS')
        with pytest.raises(ResponseError):
            client.execute_command('CF.EXISTS myfilter')

    def test_cf_mexists_command(self):
        """Test CF.MEXISTS command"""
        client = self.server.get_new_client()

        # Add some items
        assert client.execute_command('CF.ADD myfilter item1') == 1
        assert client.execute_command('CF.ADD myfilter item3') == 1

        # Check multiple items
        result = client.execute_command('CF.MEXISTS myfilter item1 item2 item3 item4')
        assert len(result) == 4
        assert result[0] == 1  # item1 exists
        assert result[1] == 0  # item2 doesn't exist
        assert result[2] == 1  # item3 exists
        assert result[3] == 0  # item4 doesn't exist

        # Wrong number of arguments
        with pytest.raises(ResponseError):
            client.execute_command('CF.MEXISTS')
        with pytest.raises(ResponseError):
            client.execute_command('CF.MEXISTS myfilter')

    def test_cf_reserve_command(self):
        """Test CF.RESERVE command"""
        client = self.server.get_new_client()

        # Basic reserve
        assert client.execute_command('CF.RESERVE myfilter 1000') == b'OK'

        # Try to reserve existing key
        with pytest.raises(ResponseError) as e:
            client.execute_command('CF.RESERVE myfilter 1000')
        assert 'exists' in str(e.value).lower() or 'busy' in str(e.value).lower()

        # Reserve with options
        assert client.execute_command('CF.RESERVE myfilter2 500 BUCKETSIZE 2 MAXITERATIONS 100 EXPANSION 2') == b'OK'

        # Invalid capacity
        with pytest.raises(ResponseError):
            client.execute_command('CF.RESERVE badfilter 0')

        with pytest.raises(ResponseError):
            client.execute_command('CF.RESERVE badfilter -1')

        # Wrong number of arguments
        with pytest.raises(ResponseError):
            client.execute_command('CF.RESERVE')
        with pytest.raises(ResponseError):
            client.execute_command('CF.RESERVE myfilter')

    def test_cf_insert_command(self):
        """Test CF.INSERT command"""
        client = self.server.get_new_client()

        # Insert with auto-create
        result = client.execute_command('CF.INSERT myfilter ITEMS item1 item2 item3')
        assert len(result) == 3
        assert all(x in [0, 1] for x in result)

        # Insert with NOCREATE on non-existent filter
        with pytest.raises(ResponseError):
            client.execute_command('CF.INSERT newfilter NOCREATE ITEMS item1')

        # Insert with NOCREATE on existing filter
        result = client.execute_command('CF.INSERT myfilter NOCREATE ITEMS item4 item5')
        assert len(result) == 2

        # Insert with custom capacity
        result = client.execute_command('CF.INSERT myfilter2 CAPACITY 500 ITEMS item1')
        assert len(result) == 1

        # Wrong format (no ITEMS keyword)
        with pytest.raises(ResponseError):
            client.execute_command('CF.INSERT myfilter3 item1 item2')

    def test_cf_insertnx_command(self):
        """Test CF.INSERTNX command"""
        client = self.server.get_new_client()

        # Insert new items
        result = client.execute_command('CF.INSERTNX myfilter ITEMS item1 item2 item3')
        assert len(result) == 3
        assert all(x == 1 for x in result)

        # Insert mix of existing and new items
        result = client.execute_command('CF.INSERTNX myfilter ITEMS item1 item4')
        assert len(result) == 2
        assert result[0] == 0  # item1 exists
        assert result[1] == 1  # item4 is new

    def test_cf_info_command(self):
        """Test CF.INFO command"""
        client = self.server.get_new_client()

        # Info on non-existent filter
        with pytest.raises(ResponseError):
            client.execute_command('CF.INFO nonexistent')

        # Create filter and get info
        assert client.execute_command('CF.RESERVE myfilter 1000') == b'OK'
        info = client.execute_command('CF.INFO myfilter')
        assert info is not None
        # Info should return array with various fields
        assert isinstance(info, (list, dict))

        # Add some items
        assert client.execute_command('CF.ADD myfilter item1') == 1
        assert client.execute_command('CF.ADD myfilter item2') == 1

        # Get specific field (if supported)
        try:
            size = client.execute_command('CF.INFO myfilter Size')
            assert size is not None
        except ResponseError:
            # Specific field queries might not be supported
            pass

        # Wrong number of arguments
        with pytest.raises(ResponseError):
            client.execute_command('CF.INFO')

    def test_cf_load_command(self):
        """Test CF.LOAD command (used for AOF rewrite and persistence)"""
        client = self.server.get_new_client()

        # CF.LOAD requires serialized data from AOF rewrite; basic arity check
        import pytest
        with pytest.raises(Exception):
            client.execute_command('CF.LOAD')
        with pytest.raises(Exception):
            client.execute_command('CF.LOAD key data extra')

    def test_argument_validation(self):
        """Test that commands properly validate arguments"""
        client = self.server.get_new_client()

        # Test invalid bucket size
        with pytest.raises(ResponseError):
            client.execute_command('CF.RESERVE badfilter 1000 BUCKETSIZE 0')

        with pytest.raises(ResponseError):
            client.execute_command('CF.RESERVE badfilter 1000 BUCKETSIZE 256')

        # Test invalid max iterations
        with pytest.raises(ResponseError):
            client.execute_command('CF.RESERVE badfilter 1000 MAXITERATIONS 0')

        # Test unknown option
        with pytest.raises(ResponseError):
            client.execute_command('CF.RESERVE badfilter 1000 UNKNOWN_OPTION 123')

    def test_error_messages(self):
        """Test that error messages are clear and helpful"""
        client = self.server.get_new_client()

        # Non-existent filter operations
        with pytest.raises(ResponseError) as e:
            client.execute_command('CF.DEL nonexistent item1')
        assert 'not found' in str(e.value).lower() or 'does not exist' in str(e.value).lower()

        # Key already exists
        assert client.execute_command('CF.RESERVE myfilter 1000') == b'OK'
        with pytest.raises(ResponseError) as e:
            client.execute_command('CF.RESERVE myfilter 1000')
        assert 'exists' in str(e.value).lower() or 'busy' in str(e.value).lower()

        # Filter full error (non-scaling)
        client.execute_command('CONFIG SET cuckoo-memory-usage-limit 500')
        with pytest.raises(ResponseError) as e:
            client.execute_command('CF.RESERVE bigfilter 10000')
        assert 'exceed' in str(e.value).lower() or 'limit' in str(e.value).lower()
