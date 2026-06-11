#!/usr/bin/env python3

"""
HW1: TEST SCRIPT

This will build your image, create a container, then run tests.
If any test fails, it will explain what went wrong.
"""

import argparse
import os
import re
import sys
from datetime import datetime

from .utils.containers import ClusterConductor, ContainerBuilder
from .utils.test_case import TestCase
from .utils.util import Logger, global_logger, log

from .tests.Asgn4_tests.PUT_data_before_PUT_view_should_return_500 import test_put_data_before_view_returns_500
from .tests.Asgn4_tests.put_replicates_only_inside_owning_shard import test_put_replicates_only_inside_owning_shard
from .tests.Asgn4_tests.test_efficient_rekey_consistent_hashing import test_efficient_rekey_consistent_hashing
from .tests.Asgn4_tests.test_efficient_rekey_shard_removal import test_efficient_rekey_on_shard_removal
from .tests.Asgn4_tests.test_get_redirects_to_owning_shard import test_get_redirects_to_owning_shard
from .tests.Asgn4_tests.test_keys_distributed_across_shards import test_keys_distributed_across_shards
from .tests.Asgn4_tests.test_missing_key_redirects_to_owning_shard import test_missing_key_redirects_to_owning_shard
from .tests.Asgn4_tests.test_multishard_view_install import test_multishard_view_install
from .tests.Asgn4_tests.test_new_node_onboarding import test_new_node_onboarding
from .tests.Asgn4_tests.test_node_moves_between_shards import test_node_moves_between_shards
from .tests.Asgn4_tests.test_old_owner_redirects_after_rekey import test_old_owner_redirects_after_rekey
from .tests.Asgn4_tests.test_put_completes_when_backup_crashed import test_put_completes_when_backup_crashed
from .tests.Asgn4_tests.test_put_completes_when_backup_partitioned import test_put_completes_when_backup_partitioned
from .tests.Asgn4_tests.test_put_redirects_to_owning_shard import test_put_redirects_to_owning_shard
from .tests.Asgn4_tests.test_replication_is_synchronous import test_replication_is_synchronous
from .tests.Asgn4_tests.test_shard_isolation_under_partition import test_shard_isolation_under_partition
from .tests.Asgn4_tests.test_view_accepts_arbitrary_shard_names import test_view_accepts_arbitrary_shard_names
from .tests.Asgn4_tests.test_within_shard_replication_failover import test_within_shard_replication_failover
from .tests.Asgn4_tests.test_writes_not_wedged_after_backup_crash import test_writes_not_wedged_after_backup_crash

# test functions
# TODO: for parallel test runs, use generated group id
CONTAINER_IMAGE_ID = "kvstore-asgn1-test"
TEST_GROUP_ID = "asgn1"


# run test set
tests = [
    TestCase("put data before view returns 500", test_put_data_before_view_returns_500),
    TestCase("put replicates only inside owning shard", test_put_replicates_only_inside_owning_shard),
    TestCase("efficient rekey consistent hashing", test_efficient_rekey_consistent_hashing),
    TestCase("efficient rekey shard removal", test_efficient_rekey_on_shard_removal),
    TestCase("get redirects to owning shard", test_get_redirects_to_owning_shard),
    TestCase("keys distributed across shards", test_keys_distributed_across_shards),
    TestCase("missing key redirects to owning shard", test_missing_key_redirects_to_owning_shard),
    TestCase("multishard view install", test_multishard_view_install),
    TestCase("new node onboarding", test_new_node_onboarding),
    TestCase("node moves between shards", test_node_moves_between_shards),
    TestCase("old owner redirects after rekey", test_old_owner_redirects_after_rekey),
    TestCase("put completes when backup crashed", test_put_completes_when_backup_crashed),
    TestCase("put completes when backup partitioned", test_put_completes_when_backup_partitioned),
    TestCase("put redirects to owning shard", test_put_redirects_to_owning_shard),
    TestCase("replication is synchronous", test_replication_is_synchronous),
    TestCase("shard isolation under partition", test_shard_isolation_under_partition),
    TestCase("view accepts arbitrary shard names", test_view_accepts_arbitrary_shard_names),
    TestCase("within shard replication failover", test_within_shard_replication_failover),
    TestCase("writes not wedged after backup crash", test_writes_not_wedged_after_backup_crash),
]


class TestRunner:
    def __init__(
        self,
        project_dir: str,
        debug_output_dir: str,
        group_id=TEST_GROUP_ID,
        thread_id="0",
    ):
        self.project_dir = project_dir
        self.debug_output_dir = debug_output_dir
        # builder to build container image
        self.builder = ContainerBuilder(
            project_dir=project_dir, image_id=CONTAINER_IMAGE_ID
        )
        # network manager to mess with container networking
        self.conductor = ClusterConductor(
            group_id=group_id,
            thread_id=thread_id,
            base_image=CONTAINER_IMAGE_ID,
            external_port_base=9000,
            log=global_logger(),
        )

    def prepare_environment(self, build: bool = True) -> None:
        log("\n-- prepare_environment --")
        # build the container image
        if build:
            self.builder.build_image(log=global_logger())
        else:
            log("Skipping build")

        # aggressively clean up anything kvs-related
        # NOTE: this disallows parallel run processes, so turn it off for that
        self.conductor.cleanup_hanging(group_only=True)

    def cleanup_environment(self) -> None:
        log("\n-- cleanup_environment --")
        # destroy the cluster
        self.conductor.destroy_cluster()
        # aggressively clean up anything kvs-related
        self.conductor.cleanup_hanging(group_only=True)


if sys.platform.startswith("win"):
    timestamp = datetime.now().strftime("test_results/%Y_%m_%d_%H-%M-%S")
else:
    timestamp = datetime.now().strftime("test_results/%Y_%m_%d_%H:%M:%S")
DEBUG_OUTPUT_DIR = os.path.join(os.getcwd(), timestamp)
os.makedirs(DEBUG_OUTPUT_DIR, exist_ok=True)
log(f"Debug output will be saved in: {DEBUG_OUTPUT_DIR}")


def create_test_dir(base_dir: str, test_set: str, test_name: str) -> str:
    test_set_dir = os.path.join(base_dir, test_set)
    os.makedirs(test_set_dir, exist_ok=True)
    test_dir = os.path.join(test_set_dir, test_name)
    os.makedirs(test_dir, exist_ok=True)
    return test_dir


"""
TEST SET: this list the test cases to run
add more tests by appending to this list
"""


TEST_SET = tests

FAIL_FAST = True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--no-build",
        action="store_false",
        dest="build",
        help="skip building the container image",
    )
    parser.add_argument(
        "--run-all",
        action="store_true",
        help="run all tests instead of stopping at first failure. Note: this is currently broken due to issues with cleanup code",
    )
    parser.add_argument(
        "--group-id",
        default=TEST_GROUP_ID,
        help="Group Id (prepended to docker containers & networks) (useful for running two versions of the test suite in parallel)",
    )
    parser.add_argument(
        "--port-offset", type=int, default=1000, help="port offset for each test"
    )
    parser.add_argument("filter", nargs="?", help="filter tests by name")
    args = parser.parse_args()

    project_dir = os.getcwd()
    runner = TestRunner(
        project_dir=project_dir,
        debug_output_dir=DEBUG_OUTPUT_DIR,
        group_id=args.group_id,
        thread_id="0",
    )
    runner.prepare_environment(build=args.build)

    if args.filter is not None:
        test_filter = args.filter
        log(f"filtering tests by: {test_filter}")
        global TEST_SET
        TEST_SET = [t for t in TEST_SET if re.compile(test_filter).match(t.name)]

    if args.run_all:
        global FAIL_FAST
        FAIL_FAST = False

    log("\n== RUNNING TESTS ==")
    run_tests = []

    def run_test(test: TestCase, gid: str, thread_id: str, port_offset: int):
        log(f"\n== TEST: [{test.name}] ==\n")
        test_set_name = test.name.lower().split("_")[0]
        test_dir = create_test_dir(DEBUG_OUTPUT_DIR, test_set_name, test.name)
        log_file_path = os.path.join(test_dir, f"{test.name}.log")

        with open(log_file_path, "w", encoding="utf-8") as log_file:
            log_file.write(f"Logs for test {test.name}\n")

            logger = Logger(files=(log_file, sys.stderr))
            conductor = ClusterConductor(
                group_id=gid,
                thread_id=f"{thread_id}",
                base_image=CONTAINER_IMAGE_ID,
                external_port_base=9000 + port_offset,
                log=logger,
            )
            score, reason = test.execute(conductor, test_dir, log=logger)

            # Save logs or any other output to test_dir
            run_tests.append(test)
            logger("\n")
            if score:
                if sys.platform.startswith("win"):
                    logger(f"[PASSED] {test.name}")
                else:
                    logger(f"✓ PASSED {test.name}")
            else:
                if sys.platform.startswith("win"):
                    logger(f"[FAILED] {test.name}: {reason}")
                else:
                    logger(f"✗ FAILED {test.name}: {reason}")
            return score

    print("Running tests sequentially")
    for test in TEST_SET:
        if not run_test(test, gid=args.group_id, thread_id="0", port_offset=0):
            if not args.run_all:
                print("--run-all not set, stopping at first failure")
                break

    summary_log = os.path.join(DEBUG_OUTPUT_DIR, "summary.log")
    with open(summary_log, "w", encoding="utf-8") as log_file:
        logger = Logger(files=(log_file, sys.stderr))
        logger("\n== TEST SUMMARY ==\n")
        for test in run_tests:
            logger(f"  - {test.name}: {'✓' if test.score else '✗'}\n")

    runner.cleanup_environment()


if __name__ == "__main__":
    main()
