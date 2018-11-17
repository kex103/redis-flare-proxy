#!/usr/bin/env python
import redis
from test_util import TestUtil

class AdminTests(TestUtil):

    def test_admin(self):
        self.start_proxy("tests/conf/timeout1.toml")

        r = redis.Redis(port=1530, decode_responses=True)
        response = r.execute_command("INFO")
        self.assertEqual(response.get('__raw__'), ["DERP"]);