# Rustproxy

A lightweight redis proxy with features suited for redis cluster management, and a focus on performance.

Features:
- Hot config swapping
- Efficient host blackout/backoff logic
- Durable deletes (TODO)

# Architecture
- Rustproxy uses mio, which introduces a Poll-and-Token event system.
- 

# Using the benchmarker

# Features
1. Sharding
    a. modula
    b. ketama
    c. random? then no shard
2. Retry?
3. Request routing.
4. 
What if, instead of an admin port, we just overloaded RUSTPROXY API?
Admin port pros/cons:
+ security can be done by just blocking the port. it's more obvious?
- An extra port to worry about.
+ Reduces possibility of concurrent connections... I guess this is a big enough deal that it should just be a single port.

# Test Plan:
- No backend
    1. If no backend is configured, then complain!
    2. Add validate-config option to program?
- Single backend
    1. test that requests get routed fine.
- Multiple backends
    1. Test that sharding is correct.
    2. 
## TODO:
1. finish benchmarking? It seems to be working. what else?
2. refactor out the written_sockets/subscribers junk. Should be explicit, or should pass it in as a thing?