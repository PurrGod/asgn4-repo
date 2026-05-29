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

from .tests.add_lowest_id_node import add_lowest_id_node
from .tests.add_node import add_node
from .tests.basic_view_change_under_partition import basic_view_change_under_partition
from .tests.concurrent_puts import concurrent_puts
from .tests.crash_scenarios import crash_scenarios
from .tests.dropped_write_after_promotion import dropped_write_after_promotion
from .tests.hello import hello_cluster
from .tests.isolate_every_node import isolate_every_node
from .tests.kill_primary import kill_primary
from .tests.node_join_leave import node_join_leave
from .tests.partitions import partitions_test
from .tests.put_and_get import put_and_get
from .tests.put_then_view_change import put_then_view_change
from .tests.redirect_semantics import redirect_semantics
from .tests.remove_non_primary import remove_non_primary
from .tests.sc_failed_put import sc_failed_put
from .tests.simple_partition import simple_partition
from .tests.simple_reelection import simple_reelection
from .tests.single_node_view import single_node_view
from .tests.strong_consistency import put_is_immediately_global
from .tests.successive_primaries import successive_primaries
from .tests.timeout_then_put import timeout_then_put
from .tests.update import update
from .tests.view_change_unblocks_put import view_change_unblocks_put
from .tests.view_change import view_change

# test functions
# TODO: for parallel test runs, use generated group id
CONTAINER_IMAGE_ID = "kvstore-asgn1-test"
TEST_GROUP_ID = "asgn1"


# run test set
tests = [
    TestCase("hello cluster", hello_cluster),
    TestCase("put and get", put_and_get),
    TestCase("updates", update),
    TestCase("strong consistency", put_is_immediately_global),
    TestCase("view change", view_change),
    TestCase("node join-leave", node_join_leave),
    TestCase("partitions", partitions_test),
    TestCase("crash scenarios", crash_scenarios),
    TestCase("redirect semantics", redirect_semantics),
    TestCase("simple partition", simple_partition),
    TestCase("simple reelection", simple_reelection),
    TestCase("sc failed put", sc_failed_put),
    TestCase("basic view change under partition", basic_view_change_under_partition),
    TestCase("add lowest id node", add_lowest_id_node),
    TestCase("add node", add_node),
    TestCase("concurrent puts", concurrent_puts),
    TestCase("dropped write after promotion", dropped_write_after_promotion),
    TestCase("isolate every node", isolate_every_node),
    TestCase("kill primary", kill_primary),
    TestCase("put then view change", put_then_view_change),
    TestCase("remove non primary", remove_non_primary),
    TestCase("successive primaries", successive_primaries),
    TestCase("timeout then put", timeout_then_put),
    TestCase("single node view", single_node_view),
    TestCase("view change unblocks put", view_change_unblocks_put)
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
