# Assignment 4: Strongly Consistent Sharded Key/Value Store

Due at 11:59 PM on 2026-05-29.


## Overview

The goal of this assignment is to extend [assignment 3](./assignment3.md) with
sharding.

The entirety of assignment 3 still applies, with the added complexity of
multiple shards specified in the view change. One advantage of sharding is that
it can spread out load of the system across more machines, provided clients
aren't exclusively writing to one key. In such a worst-case scenario, only one
shard would be being used. The other advantage of sharding is that each replica
only needs to store data from its shard instead of all replicas storing the
entire key-value store. The last advantage is that even if one shard has a
downed node, keys that are processed by the other shards can still be written to
and read from.

The disadvantage of shards is that it adds some complexity to your
implementation. Most of this complexity will cluster around your view change
logic since machines can change shards and some shards may even completely be
removed. Additionally your logic about which node to redirect requests to will
become more complex.

## Efficient Rearrangement of Keys

Approximately 5-10% of your grade for this assignment will be based on whether
your implementation efficiently rearranges data across shards during a view
change. For example, if there was four shards before the view change, each with
one node, and five shards after the view change, still each with one node, then
only about 20% of the keys should be moved across the shards. 

We'll have some tolerance in our testing of this. That said, a random shuffle of
all keys in the above case would on average result in about 80% of the keys
being moved which is easily noticeable by tracking the network bandwidth during
a view change.

To make shuffling data around easier for this assignment, we guarantee that
there will be no partitions across the network during a view change that changes
the shard membership of a node to so that it's easier to efficiently sync data
between shards.

### Consistent Hashing

To accomplish this, consistent hashing is your friend! How to apply consistent
hashing for sharding will likely be covered in class. Additionally it is covered
in the [Chord paper by Stoica et.
al.](https://dl.acm.org/doi/10.1145/964723.383071). Pay careful attention to
section 4.2, especially regarding the use of virtual nodes. 

## Setup

Have one group member navigate to their assignment 1 repository directory on
their machine. Be sure that the repository is a) shared with zejones and b) **is
NOT in the cse-138 Student Work group.**

Then run the following:

```sh
git checkout asgn3
# rename asgn2 upstream to be called upstream-asgn2
git remote rename upstream upstream-asgn3
# set the upstream to be the assignment-2 spec
git remote add upstream git@git.ucsc.edu:cse138/w26-assignment-4.git
# checkout from asgn3 to begin asgn4
git checkout -b asgn4
# pull in the assignment 4 spec into the asgn4 branch
git fetch upstream main
git rebase upstream/main
git push --all
```

If you follow the above setup then you'll be able to easily fetch future updates
to this repository by running:

```sh 
git fetch upstream main
git rebase upstream/main
```

To ensure rebases go smoothly, we strongly recommend against editing anything in
the `guidelines` or `provided_tests` directories. You of course are more than
welcome to copy anything from `provided_tests` into another directory such as
`tests` if you'd like to add onto the `provided_tests`. Since `provided_tests`
is a python module, you are also welcome to just copy `__main__.py` out of
`provided_tests` and then change the imports at the top of the file like so:

```diff
-from .tests.hello import hello_cluster
-from .tests.put_and_get import put_and_get
-from .tests.update import update
-from .utils.containers import ClusterConductor, ContainerBuilder
-from .utils.test_case import TestCase
-from .utils.util import Logger, global_logger, log
+from provided_tests.tests.hello import hello_cluster
+from provided_tests.tests.put_and_get import put_and_get
+from provided_tests.tests.update import update
+from provided_tests.utils.containers import ClusterConductor, ContainerBuilder
+from provided_tests.utils.test_case import TestCase
+from provided_tests.utils.util import Logger, global_logger, log
```

You'll also be able to import from provided_tests in your `tests` directory.


## Dependencies

You are allowed to use dependencies which do not help with the primary point
of the assignment. As a general rule, if your library is distributed systems
specific then you probably shouldn't be using it for your server (you are
welcome to use distributed system libraries ONLY for testing however). For
instance, you are allowed to use:

- your language's standard library
- an HTTP library
- a logging library
- a serialization/deserialization library (such as for json)
- a distributed system testing library (such as turmoil)
- a rpc library such as gRPC/Protobuf

If you have a library that you want to use but which you feel might be
borderline, please ask the course staff!

## Provided Tests 

Within this repository exists a basic Python tester. To setup your python
environment run the following:

```sh 
python -m venv .venv 
source .venv/bin/activate 
python -m pip install -r provided_tests/requirements.txt 
```

Note: If you're on MacOS you might need to use the `python3` command instead of
the `python` command.

To run the test do the following:

```sh 
# if this hasn't been run in the terminal session thus far 
source .venv/bin/activate 
# you can also do ENGINE=podman if, like me, you prefer using Podman over Docker
ENGINE=docker python -m provided_tests
```

The provided tests are exactly the same as the provided tests for assignment 3.
Notably the test suite's ClusterConductor already has functions to add and
remove nodes from shards and create new shards (add_shard, remove_shard,
add_node_to_shard, remove_node_from_shard). That said, it might be worth writing
additional utility functions to move a node between shards. Also the provided
crash_machine function currently will only remove the crashed node from the
"defaultShard" so you might want to avoid using that function.

#### Suggested Expansions for the Test Suite

The specification is the ultimate source of truth for how your service should
behave and the tests currently in the provided_tests barely scratch the surface
of testing conformance to the specification. That said, here are some suggested
areas of expansion/modification for the provided test suite:
- In addition to partitions, your service MUST tolerate replicas crashing and
  handle new replicas coming online via view changes. You may need to poke
  around the testing utils to figure out how to kill servers and spawn new ones.
  Remember to broadcast the view when a new server is spawned!
- Handle 307 redirects properly by replacing the IP address returned from the
  server in the Location header into localhost:<PORT>, where PORT is the port of
  the container which has the returned IP address.
- Multithreading the test suite 
- Trying out [property-based testing](https://en.wikipedia.org/wiki/Property_testing)

## Required Files/Documentation

### Dockerfile
You must include a Dockerfile in the root of the repository which has the steps
to build your container. 

Your Dockerfile MUST build for both x86_64 and arm64 architectures. Most groups
did this correctly for the first assignment and the submissions that did not do
this correctly had a note left on their assignment about how to fix this.
Generally unless you have a custom target in your Dockerfile everything should
"just work" on both x86_64 and arm64.

**Make sure you capitalize the "D" in Dockerfile filename!**

### README.md

You must include a README.md file in the root of the repository which explains:
1. your dependencies
2. How you got new nodes up to speed
3. How you chose which keys map to which shards
3. If you implemented consistent hashing, explain your implementation
4. Your server's architecture (what directories/files do what)
5. how you tested your code (cite your classmates here if you used their tests!). Note that you do not need to describe every test, but you should provide a high-level overview of your testing strategy.
6. which group members did what
7. No more than one paragraph on how your group used LLMs to complete the assignment

If you'd like you're welcome to link out of the README.md for better
organization (e.g. have each group member write what they did in a separate
markdown file) so long as your README links to those other markdown files.

Everything in your README or transitively linked from your README must be
written by a human. No LLM text allowed!

### team.json

You must also include a team.json file in the root of your repository structured
like so with all your groupmates' emails:

```json
{ "members": ["your_cruzid@ucsc.edu", "another_cruzid@ucsc.edu"] }
```

## Submissions

To submit your repository, fill out [this google
form](https://docs.google.com/forms/d/e/1FAIpQLSeKb1GuKbCYwocyxqVN_N3uvVLD-TuU7qMK--WWZyN2XYxAQw/viewform?usp=publish-editor)
with your repository URL and your commit hash. Please ensure you are signed into
Google with your UCSC email, otherwise this form will be inaccessible. You are
welcome to resubmit up until the due date. After the due date, resubmissions are
no longer accepted. First-time submissions after the due date will count against
your grace day and may be docked credit for being late (see the syllabus for the
late policy).

Grades and feedback will be provided via git pushes to your repository via the 
zejones-asgn4-feedback branch.

## AI Policy

You are welcome to use LLMs (at your own risk) to help complete the coding part
of this assignment. All non-code in your repository, including code comments and
markdown files, MUST be written by you or your group mates manually.

### Some Unsolicited Advice on LLMs

Be warned that LLMs often make subtle mistakes and such mistakes are ultimately
your group's responsibility to catch. Such subtle mistakes tend to quickly stack
on one another and will exponentially decrease your codebase's quality if you
do not catch them quickly.

I don't believe there is enough context spread across this document, the
specification, and the provided test suite for an LLM to understand how to even
properly test strong consistency, let alone write a conformant implementation.
There is a lot of subtlety in how your service MUST act and I doubt any LLM will
understand such subtleties fully just based on the documentation provided here
alone.

I think part of what makes this course compelling, especially as LLMs threaten
the software developer job market, is that reasoning about distributed systems
is a challenging and precise engineering task that is difficult for any
non-expert to do well which also makes it incredibly difficult to outsource to
an LLM. 

Furthermore, this assignment is great for learning how to reason
about distributed systems so I would be very careful about accidentally
delegating that learning process to an LLM.
