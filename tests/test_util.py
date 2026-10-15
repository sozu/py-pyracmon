import pytest
from pyracmon.util import *


class TestKeyToIndex:
    def test_str_keys(self):
        v = key_to_index(dict(a=1, b=2, c=3), ["b", "c", "a"])
        assert v == {2:1, 0:2, 1:3}

    def test_str_int(self):
        v = key_to_index({"a":1, 3:2, "c":3}, ["b", "c", "a"])
        assert v == {2:1, 3:2, 1:3}
