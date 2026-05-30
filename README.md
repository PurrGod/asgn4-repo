# Strongly Consistent Key/Value Store (Assignment 3)

This repository implements a strongly consistent HTTP key/value store for CSE 138 Assignment 3.


## Dependencies
	- fastapi
	- uvicorn
	- httpx

## Design
- Bringing new nodes up to speed: the implementation handles view changes by ensuring an entering node is added to the cluster view and receiving the current state from existing replicas before the view is considered acknowledged. This ensures that once a view change completes, acknowledged PUTs are reflected on all replicas in the updated view.
- Roles and coordination: the implementation sequences acknowledged PUTs via a coordinating replica so that acknowledgements define a total order of updates for each key. Replicas may reply with 307 redirects to route clients to the appropriate coordinator when necessary.
- Architecture: the codebase is intentionally small and concentrates server logic in `main.py` and supporting modules. Tests and test utilities live in `provided_tests` and `utils` respectively.

Testing strategy

- Start with the provided basic tests in `provided_tests` to validate core PUT/GET behavior and view/change handling.
- Expand testing to exercise crash scenarios, partitions, and view changes. Use the `ClusterConductor` utilities in `provided_tests/utils/containers.py` to simulate crashes, partitions, and node joins.
- Verify strong consistency by ensuring that once a PUT is acknowledged, subsequent GETs from any replica return the acknowledged value.
- Test redirect semantics: confirm that followers return 307 to the primary for both PUTs and GETs, and that a view change promoting a follower stops the redirects.
- Test crash and primary failover: kill the primary mid-run and verify the surviving follower is promoted and still serves all previously acknowledged data.
- Test concurrent PUTs to the same key from multiple nodes simultaneously; verify all nodes converge to the same final value. (Tests designed by iyyam/ajagathe/brcalcan.)
- Test adding a node with the lowest ID so it becomes the new primary; verify it receives a full state transfer from existing nodes before serving reads. (Tests designed by iyyam/ajagathe/brcalcan.)
- Test that a pending PUT blocked by an unreachable replica eventually commits once the view is shrunk to exclude that replica (view change unblocks stuck PUT). (Tests designed by iyyam/ajagathe/brcalcan.)
- Test successive primary promotions across multiple view changes to ensure clock tracking stays consistent and the latest acknowledged value is always returned. (Tests designed by iyyam/ajagathe/brcalcan.)
- Test dropped writes after promotion: an unacknowledged write that reached only some replicas before a partition must not be visible after a new primary is elected. (Tests designed by iyyam/ajagathe/brcalcan.)

Contributors and roles

- Arhan: implemented the initial server boilerplate and passed the basic provided tests.
- Akhilesh: performed in-depth testing focused on strong consistency; diagnosed and fixed edge cases uncovered by partition/crash tests.
- Marcus: ran final verification and test sweeps across the provided test suite and ensured the implementation follows assignment guidelines.

AI usage

We used an LLM only as a brainstorming aid during development. 
