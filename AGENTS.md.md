# Project Guidelines

This is a caching project compatible with the Redis RESP protocol and implements the Raft consensus algorithm.

Every code change must preserve Redis protocol semantics, including:

* Argument parsing
* Command execution behavior
* Response generation

Every code change must avoid any noticeable performance regression. Do not introduce expensive or performance-intensive operations into the execution path of all commands merely to support a small number of commands.

Before modifying any code, read all design documents under the `docs` directory.

All Redis command implementations must be placed under the `protocol` directory.

Command implementations must use the appropriate trait based on their access pattern:

* Single-key reads must implement the `ReadCommand` trait.
* Multi-key reads must implement the `MultiReadCommand` trait.
* Single-key read-and-write commands must implement the `ComputeCommand` trait.
* Multi-key reads with a single-key write should implement the `MultiReadComputeCommand` trait.

If you notice bugs unrelated to the current modification, report them first instead of fixing them directly.

`src/mocha` contains the cache implementation for scheduled expiration based on a logical clock and timing wheel.

The system is still in alpha. Changes do not need to maintain compatibility with previous versions, including file version numbers and similar versioning concerns.
