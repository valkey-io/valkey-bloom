import pytest
import valkey
from valkey.exceptions import ValkeyError
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytests.conftest import resource_port_tracker

class TestValkeyBloomCompatibility(ValkeyBloomTestCaseBase):
    """
        This test file is aiming to test the valkey-bloom compatibility with redis-bloom.
        All test cases and helper functions in this class are from https://github.com/valkey-io/valkey-py/blob/main/tests.
    """

    def get_protocol_version(self, r):
        if isinstance(r, valkey.Valkey) or isinstance(r, valkey.asyncio.Valkey):
            return r.connection_pool.connection_kwargs.get("protocol")
        elif isinstance(r, valkey.cluster.AbstractValkeyCluster):
            return r.nodes_manager.connection_kwargs.get("protocol")

    def assert_resp_response(self, r, response, resp2_expected, resp3_expected):
        protocol = self.get_protocol_version(r)
        if protocol in [2, "2", None]:
            assert response == resp2_expected
        else:
            assert response == resp3_expected

    def intlist(self, obj):
        return [int(v) for v in obj]

    def test_create(self):
        """Test CREATE/RESERVE calls"""
        decoded_r = self.server.get_new_client()
        assert decoded_r.bf().create("bloom", 0.01, 1000)
        assert decoded_r.bf().create("bloom_e", 0.01, 1000, expansion=1)
        assert decoded_r.bf().create("bloom_ns", 0.01, 1000, noScale=True)

    def test_bf_add(self):
        decoded_r = self.server.get_new_client()
        assert decoded_r.bf().create("bloom", 0.01, 1000)
        assert 1 == decoded_r.bf().add("bloom", "foo")
        assert 0 == decoded_r.bf().add("bloom", "foo")
        assert [0] == self.intlist(decoded_r.bf().madd("bloom", "foo"))
        assert [0, 1] == decoded_r.bf().madd("bloom", "foo", "bar")
        assert [0, 0, 1] == decoded_r.bf().madd("bloom", "foo", "bar", "baz")
        assert 1 == decoded_r.bf().exists("bloom", "foo")
        assert 0 == decoded_r.bf().exists("bloom", "noexist")
        assert [1, 0] == self.intlist(decoded_r.bf().mexists("bloom", "foo", "noexist"))

    def test_bf_insert(self):
        decoded_r = self.server.get_new_client()
        assert decoded_r.bf().create("bloom", 0.01, 1000)
        assert [1] == self.intlist(decoded_r.bf().insert("bloom", ["foo"]))
        assert [0, 1] == self.intlist(decoded_r.bf().insert("bloom", ["foo", "bar"]))
        assert [1] == self.intlist(decoded_r.bf().insert("captest", ["foo"], capacity=10))
        assert [1] == self.intlist(decoded_r.bf().insert("errtest", ["foo"], error=0.01))
        assert 1 == decoded_r.bf().exists("bloom", "foo")
        assert 0 == decoded_r.bf().exists("bloom", "noexist")
        assert [1, 0] == self.intlist(decoded_r.bf().mexists("bloom", "foo", "noexist"))
        info = decoded_r.bf().info("bloom")
        self.assert_resp_response(
            decoded_r,
            2,
            info.get("insertedNum"),
            info.get("Number of items inserted"),
        )
        self.assert_resp_response(
            decoded_r,
            1000,
            info.get("capacity"),
            info.get("Capacity"),
        )
        self.assert_resp_response(
            decoded_r,
            1,
            info.get("filterNum"),
            info.get("Number of filters"),
        )

    def test_bf_info(self):
        decoded_r = self.server.get_new_client()
        expansion = 4
        # Store a filter
        decoded_r.bf().create("nonscaling", "0.0001", "1000", noScale=True)
        info = decoded_r.bf().info("nonscaling")
        self.assert_resp_response(
            decoded_r,
            None,
            info.get("expansionRate"),
            info.get("Expansion rate"),
        )

        decoded_r.bf().create("expanding", "0.0001", "1000", expansion=expansion)
        info = decoded_r.bf().info("expanding")
        self.assert_resp_response(
            decoded_r,
            4,
            info.get("expansionRate"),
            info.get("Expansion rate"),
        )

        try:
            # noScale mean no expansion
            decoded_r.bf().create(
                "myBloom", "0.0001", "1000", expansion=expansion, noScale=True
            )
            assert False
        except ValkeyError:
            assert True

    def test_bf_card(self):
        decoded_r = self.server.get_new_client()
        # return 0 if the key does not exist
        assert decoded_r.bf().card("not_exist") == 0

        # Store a filter
        assert decoded_r.bf().add("bf1", "item_foo") == 1
        assert decoded_r.bf().card("bf1") == 1

        # Error when key is of a type other than Bloom filtedecoded_r.
        with pytest.raises(valkey.ResponseError):
            decoded_r.set("setKey", "value")
            decoded_r.bf().card("setKey")

    """
        This test is commented in the valkey-py/tests/test-bloom.py due to
        pipeline has not yet implemented in valkey-py BFBloom class.
    """
    # def test_pipeline(self):
    #     decoded_r = self.server.get_new_client()
    #     pipeline = decoded_r.bf().pipeline()
    #     assert not decoded_r.bf().execute_command("get pipeline")
    #
    #     assert decoded_r.bf().create("pipeline", 0.01, 1000)
    #     for i in range(100):
    #         pipeline.add("pipeline", i)
    #     for i in range(100):
    #         assert not (decoded_r.bf().exists("pipeline", i))
    #
    #     pipeline.execute()
    #
    #     for i in range(100):
    #         assert decoded_r.bf().exists("pipeline", i)
