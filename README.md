Rustproxy
==============

A fast, lightweight redis proxy with support for Redis Cluster. This enables you to horizontally scale up your redis usage safely, easily, and without downtime.

Rustproxy supports the redis protocol, and can be dropped in between an existing client/server without any changes.

Performance
==============

Rustproxy 0.0.1  
Twemproxy 4.4.
Redis-cluster 
(base comparison of redis)

Benchmarker can be built with the --benchmarker target.

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

Compatible with traffic that points to a redis server.
Easy to setup, and easy to switch over from a traditional redis setup without any service outage or data inconsistency!

# Requirements
- Requirements to build... OR use an existing package!
- Install rust.

# How to build
- Use cargo build --release or --debug.

# Packages

# Installation
With cargo: cargo install rustproxy

Or download binary: 

- Download package

# Getting started

- Look at guide.

# Upgrading from an existing redis usage
- Upgrade your redis setup without any downtime or updating your clients!
- See guide here.

# Benchmarking comparison

Compare with base redis?
Compare with twemproxy?
Compare with redis cluster?



# Want to upgrade?
Pro and Enterprise versions?

# Issues?
Report them here.

# License

LGPL3

# Author
- Written by Kevin Xiao.


==== Differentiaton:
Free: Twemproxy + cluster.
Pro: hot reload. Stats/metrics? consistent hashing. authentication. select db?
Enterprise: Cloud cluster. Migration tools? Convert twemproxy config to proxy config.


======
Landing page

1. Headline: "Scale up your redis usage. No downtime. No performance loss. No data loss. No pain."
2. Call to Action: "Sign up for blah, blah now" for interested visitors. Cold visitors, can click on "Take a look at how to upgrade an existing setup."



## Random notes

# Architecture?
- Rustproxy uses mio, which introduces a Poll-and-Token event system.


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
