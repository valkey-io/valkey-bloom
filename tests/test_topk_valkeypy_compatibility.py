import pytest
from valkey_bloom_test_case import SkipSeedParameterizationMixin, ValkeyBloomTestCaseBase
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401

class TestValkeyTopKCompatibility(SkipSeedParameterizationMixin, ValkeyBloomTestCaseBase):
    """
        Tests TopK compatibility with valkey-py's high-level client API.
        Adapted from https://github.com/valkey-io/valkey-py/blob/main/tests/test_bloom.py
    """

    def test_topk(self):
        decoded_r = self.server.get_new_client()

        assert decoded_r.topk().reserve("topk", 3, 50, 4, 0.9)
        add_return = decoded_r.topk().add(
            "topk",
            "A", "B", "C", "D", "E", "A", "A", "B", "C", "G",
            "D", "B", "D", "A", "E", "E", 1,
        )
        assert 17 == len(add_return)
        for entry in add_return:
            assert entry is None or isinstance(entry, str)

        query_return = decoded_r.topk().query("topk", "A", "B", "C", "D", "E", "F", "G")
        assert 7 == len(query_return)
        for entry in query_return:
            assert entry in [0, 1]
        assert 0 == query_return[5]  # F never added
        assert 0 == query_return[6]  # G never added

        with pytest.deprecated_call():
            count_return = decoded_r.topk().count("topk", "A", "B", "C", "D", "E", "F", "G")
        assert 7 == len(count_return)
        assert 0 == count_return[5]  # F never added

        # test full list
        assert decoded_r.topk().reserve("topklist", 3, 50, 3, 0.9)
        decoded_r.topk().add(
            "topklist",
            "A", "B", "C", "D", "E", "A", "A", "B", "C", "G",
            "D", "B", "D", "A", "E", "E",
        )
        listed = decoded_r.topk().list("topklist")
        assert len(listed) <= 3
        assert "A" in listed

        listed_wc = decoded_r.topk().list("topklist", withcount=True)
        assert len(listed_wc) == 2 * len(listed)

        info = decoded_r.topk().info("topklist")
        assert 3 == info["k"]
        assert 50 == info["width"]
        assert 3 == info["depth"]
        assert 0.9 == round(float(info["decay"]), 1)

    def test_topk_incrby(self):
        decoded_r = self.server.get_new_client()
        assert decoded_r.topk().reserve("topk", 3, 10, 3, 0.9)
        incr_return = decoded_r.topk().incrby("topk", ["bar", "baz", "42"], [3, 6, 2])
        assert 3 == len(incr_return)
        for entry in incr_return:
            assert entry is None or isinstance(entry, str)

        incr_return = decoded_r.topk().incrby("topk", ["42", "xyzzy"], [8, 4])
        assert 2 == len(incr_return)

        with pytest.deprecated_call():
            count_return = decoded_r.topk().count("topk", "bar", "baz", "42", "xyzzy", 4)
        assert 5 == len(count_return)
        assert 0 == count_return[4]   # "4" never added
        assert 10 == count_return[2]  # "42" incremented by 2+8=10
