#!/usr/bin/env python
from test_util import TestUtil

class ClusterTests(TestUtil):

# Test cluster backends, no timeout. Verify sharding is correct.
    def test_cluster_no_timeout(self):
        # use redis cluster configs
        # place runtime configs in test/tmp folder
        # make surre ports 7000-7002 and 17000-17002 are open.
        # start up redis servers
        # start up redflareproxy
        # check sharding.
        self.start_redis_cluster_server(7000)
        self.start_redis_cluster_server(7001)
        self.start_redis_cluster_server(7002)
        self.initialize_redis_cluster([7000, 7001, 7002])
        self.start_proxy("tests/conf/cluster1.toml")

        #TestUtil.verify_redis_connection(1533)

        TestUtil.populate_redis_key(1533, "key1")
        self.assert_redis_key(1533, "key1")

        TestUtil.populate_redis_key(1533, "key2")
        self.assert_redis_key(1533, "key2")

    def test_cluster_timeout(self):
        pass
        # Test that if the cluster's only backends time out on the slotsmap request, it will resend it.

        # Test that if a cluster times out many times, it will be blacked out?

        # What about case where cluster is permanently down on some key range, should it be blacked out? Can we only black out relevant range?

        # What about case where cluster is permnanently down on all key range. Maybe it should be blacked out.

        # For pinging that range for health, it should use the correct key for that key slot...