# Assignment 1: Key/Value Store
Due at 09:50 AM on 2026-04-14.

The goal of this assignment is to write an HTTP server which follows the [specification](./specification.md) which specifies a key/value store. For this assignment, only one server will be run.

## Setup

We strongly recommend building your assignment on top of this repository. You can do so like so:

```sh
git clone THIS_REPO_LINK # however you normally clone repositories, could be either by HTTP or SSH
git remote rename origin upstream # renames the origin "remote" (this repository) to be called "upstream"
git remote add origin GROUP_REPO_LINK # your repository on Gitlab
git push --set-upstream origin --all
```

If you follow the above setup then you'll be able to easily get updates to this repository by simply running:
```sh
git rebase upstream/main
```

To ensure rebases go smoothly, we strongly recommend against editing anything in the `guidelines` or `provided-tests` directories. You of course are more than welcome to copy anything from `provided-tests` into another directory such as `tests` if you'd like to add onto the `provided-tests`.


## Provided Tests 

Within this repository exists a basic Python tester. To setup your python environment run the following:
```sh
python -m venv .venv
source .venv/bin/activate
python -m pip install -r provided-tests/requirements.txt
```

To run the test do the following:

```sh
source .venv/bin/activate # if this hasn't been run in the terminal session thus far
ENGINE=docker python provided-tests/test.py # you can also do ENGINE=podman if, like me, you prefer using Podman over Docker
```

Note: If you're on MacOS you might need to use the `python3` command instead of the `python` command.

The provided tests are by no means meant to be as thorough as the grading test suite so you are strongly encouraged to build on our test suite (e.g. add more tests, make the tests run in parallel, add fuzzing (randomized) tests, etc.). Remember that you should do modifications to the test suite in another directory such as `tests` instead of editing them directly!
