# Assignment 3: Strongly Consistent Key/Value Store

Due at 11:40 AM on 2026-05-14.


## Overview

The goal of this assignment is to write an HTTP server that follows the
[specification](./specification.md) that specifies a strongly consistent
key/value store. For this assignment several servers will be run, servers might
crash, partitions between servers might occur, and new servers might be
introduced into the service. How your service MUST handle such interruptions *is
not* specified; however, how your service MUST always behave (even under
partitions/with crashes) *is* specified. It is up to your group to figure out
how to satisfy the behavior specified even under partitions/crashes/new servers
joining.

The one nicety we guarantee is that once a server has crashed it is guaranteed
that it SHALL NOT start again. This also means it is not useful to store your
key/value pairs to disk.

Note that a server partitioned from all other nodes until the end of time is
indistinguishable from a crashed server and that there's no reliable way for a
replica to tell whether a replica has crashed or is merely partitioned for
forever.

## Strong Consistency Definition

Strong consistency informally guarantees that interacting with a service with
multiple nodes MUST behave identically to interacting with a single node, even
from the perspective of a global observer, with the one notable exception that
the service MAY take an unbounded amount of time to reply to PUT and GET
requests if necessary to uphold strong consistency.

Formally speaking, strong consistency guarantees that once any PUT request has
been acknowledged, i.e. a view change or a key PUT, all replicas MUST be storing
that value. Such a guarantee ensures that all acknowledged values are replicated
on all replicas. Furthermore, all GET requests made to any node MUST return the
event most recently acknowledged by any server. In other words, values for a
given key are totally ordered based on the order your nodes acknowledge the
values and you MUST return the latest PUT value for a given key.

## Sacrifices in Availability

Ultimately the most available version of this assignment is arguably assignment
1 because it always replies so long as that single server is alive.

For assignment 2 we reduced availability of GET requests only to the point
necessary to uphold causal convergence by having the service occasionally return
503 responses.

For assignment 3 we further reduce availability such that if any server becomes
partitioned from any other server then the service MAY stop serving PUT and GET
requests altogether if necessary to ensure strong consistency. Notably in the
case where all servers are alive but partitioned from one another, at least one
node MUST still reply to GET requests. Ultimately, for assignment 3 you MUST
only sacrifice availability if strictly necessary to uphold strong consistency.

## Stateless Client

In contrast to assignment 2, clients for assignment 3 are stateless and SHALL
NOT keep track of cookies. This is intuitive because the service MUST behave the
same as a single machine from the perspective of a global observer, with the
notable exception of 307 responses.

## 307 (Temporary Redirect) Responses

A 307 HTTP response code means "Temporary Redirect" and is accompanied with a
`Location` header [[mdn
reference](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/307)].
When a client gets a 307 HTTP response code that client SHALL re-send the
request to the url in the Location header with the same method and body.

If a replica replies to a request with a 307 response then the replica with the
address in the `Location` header MUST NOT reply with a 307 response unless a
client has sent a view change made since the redirect reply was made.

Using this HTTP response code MAY prove useful in upholding strong consistency.

## View Changes

Without view changes, any server crashing could prevent future PUT requests from
being made and furthermore a specific server crashing could prevent GET requests
if the node responsible for handling GET requests was the one which crashed.
Obviously servers MAY crash and ideally a system administrator can trivially
reconfigure the cluster without the crashed node so that the cluster can
continue to handle more requests.

Ultimately the view change frees the service from ensuring strong consistency
with nodes no longer in the view.

Similarly, a system administrator might want to dynamically add and remove nodes
from the cluster in order to dynamically scale the service in response to load.
That said, we assume the system administrator is willing to wait up to N seconds
for a view change to be processed, notably to allow the other replicas to bring
any new node up to date. All replicas MAY stop replying to requests after a view
change if necessary to uphold strong consistency.

Be sure to read through the specification's rules around view changes, found in 
the "PUT `/view`" section, very carefully!

## Setup

Have one group member navigate to their assignment 1 repository directory on
their machine. Be sure that the repository is a) shared with zejones and b) **is
NOT in the cse-138 Student Work group.**

Then run the following:

```sh
git checkout main
# freeze the current commit corresponding to assignment 2 to its own branch
git branch asgn2
# rename asgn2 upstream to be called upstream-asgn2
git remote rename upstream upstream-asgn2
# set the upstream to be the assignment-2 spec
git remote add upstream git@git.ucsc.edu:cse138/w26-assignment-3.git
# go to the asgn1 branch
git checkout asgn1
# checkout from asgn1 to begin asgn3
git checkout -b asgn3
# pull in the assignment 3 spec into the asgn3 branch
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

The provided tests are the exactly the same as the provided tests for assignment
2 with the removal of the `bob_smells` test and the KvsMultiClient. This means
creating partitions works the same way it did for assignment 2. Notably there is
also the crash_machine() command within the ClusterConductor which will crash
the node referenced and remove it from the view (but WILL NOT rebroadcast the view).

#### Suggested Expansions for the Test Suite

The specification is the ultimate source of truth for how your service should
behave and the tests currently in the provided_tests barely scratch the surface
of testing conformance to the specification. That said, here are some suggested
areas of expansion/modification for the provided test suite:
- In addition to partitions, your service MUST tolerate replicas crashing and
  handle new replicas coming online via view changes. You may need to poke
  around the testing utils to figure out how to kill servers and spawn new ones.
  Remember to broadcast the view when a new server is spawned!
- Multithreading the test suite 
- Trying out [property-based testing](https://en.wikipedia.org/wiki/Property_testing)

## Required Files/Documentation

### Dockerfile
You must include a Dockerfile in the root of the repository which has the steps
to build your container. 

Your Dockerfile MUST build for both x86_64 and arm64 architectures. Most groups
did this correctly for the first assignment and the submissions that did not do this
correctly had a note left on their assignment about how to fix this. Generally
unless you have a custom target in your Dockerfile everything should "just work"
on both x86_64 and arm64.

**Make sure you capitalize the "D" in Dockerfile filename!**

### README.md

You must include a README.md file in the root of the repository which explains:
1. your dependencies
2. How you got new nodes up to speed
3. How you chose which nodes to play which roles and what roles they played
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
form](https://docs.google.com/forms/d/e/1FAIpQLSfevaURMsmUKdZUA3OVWXOH3tumSldrbDfHue2eibH0ScahAg/viewform?usp=publish-editor)
with your repository URL and your commit hash. Please ensure you are signed into
Google with your UCSC email, otherwise this form will be inaccessible. You are
welcome to resubmit up until the due date. After the due date, resubmissions are
no longer accepted. First-time submissions after the due date will count against
your grace day and may be docked credit for being late (see the syllabus for the
late policy).

Grades and feedback will be provided via git pushes to your repository via the 
zejones-asgn3-feedback branch.

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
