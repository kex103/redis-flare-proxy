#!/usr/bin/env python
from test_util import TestUtil

class ShardingTests(TestUtil):

    def test_conhashing(self):
        pass
        # Test that consistent hashing works as expected
        # 1. Test that hashing is similar to twemproxy. (4 backends)
        # 2. Verify that when one backend is ejected (with auto_eject_host), all requests to other backends remain, but requests to the failed one gets moved.
        # 3. Verify the same happens when another backend is ejected. Verify it's the same as twemproxy.
        # 4. Verify recover of 2nd
        # 5. Verify recovery of first.
        # TODO: Set up a test while a constant stream of redis requests occurs. We want to make sure the switch is clean.