RedFlare proxy
==============

A fast, lightweight redis proxy with support for Redis Cluster. This enables you to horizontally scale up your redis usage safely, easily, and without downtime.

RedFlare proxy supports the redis protocol, and can be dropped in between an existing client/server without any changes.


Features
==============

- Redis cluster support
- Hot config swapping
- Efficient host blackout/backoff logic
- Fast performance
- Multiple server pools
- Pipelined requests/responses
- Consistent hashing option
- Stats monitoring

Requirements
==============
Rust > 1.30.0

How to build
==============
    cargo build --release --bin redflareproxy

License
==============

See [LICENSE](https://github.com/kex103/redflare/blob/master/LICENSE)

# Author
- Written by Kevin Xiao (kex.code.dev@gmail.com)
