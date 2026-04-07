# Assignment 1: Key/Value Store 

Due at 11:40 AM on 2026-04-14.

The goal of this assignment is to write an HTTP server which follows the
[specification](./specification.md) which specifies a key/value store. For this
assignment, only one server will be run and the server will be assumed to have
a 100% uptime. This means it is not useful to store your key/value pairs to disk.
This also makes this assignment the one and only non-distributed assignment for 
this course.

## Setup

You must use [UCSC's Gitlab instance](https://git.ucsc.edu) for your git
repository. To setup your git repository do the following:
- Create a blank and **private** git repository titled something along the lines
  of "cse138-assignment-1". 
  - Be sure to **uncheck** "Initialize repository with a README"!
- Invite the member "zejones" to your repository with Maintainer permissions
  through the repository settings.

Then run the following locally:

```sh
git clone git@git.ucsc.edu:cse138/w26-assignment-1.git
# renames the origin "remote" (this repository) to be called "upstream" 
git remote rename origin upstream 
# your newly created empty repository's ssh address on git.ucsc.edu
git remote add origin YOUR_REPO_LINK
# makes your GROUP_REPO_LINK the repository you're pushing to
git push --set-upstream origin --all
```

If you follow the above setup then you'll be able to easily fetch future updates to this
repository by simply running:

```sh 
git fetch upstream main
git rebase upstream/main 
```

To ensure rebases go smoothly, we strongly recommend against editing anything in
the `guidelines` or `provided-tests` directories. You of course are more than
welcome to copy anything from `provided-tests` into another directory such as
`tests` if you'd like to add onto the `provided-tests`.


## Dependencies

You are allowed to use dependencies which do not help with the primary point
of the assignment. As a general rule, if your library is distributed systems
specific then you probably shouldn't be using it for your server (you are
welcome to use distributed system libraries ONLY for testing however). For
instance, you are allowed to use:

- your language's standard library
- an HTTP library
- a logging library
- a json serialization library
- a distributed system testing library (such as turmoil)

If you have a library that you want to use but which you feel might be
borderline, please ask the course staff!

## Provided Tests 

Within this repository exists a basic Python tester. To setup your python
environment run the following: 

```sh 
python -m venv .venv 
source .venv/bin/activate 
python -m pip install -r provided-tests/requirements.txt 
```

Note: If you're on MacOS you might need to use the `python3` command instead of
the `python` command.

To run the test do the following:

```sh 
# if this hasn't been run in the terminal session thus far 
source .venv/bin/activate 
# you can also do ENGINE=podman if, like me, you prefer using Podman over Docker
ENGINE=docker python -m provided-tests
```

The provided tests are by no means meant to fully test the specification so you
are strongly encouraged to build on our test suite (e.g. add more tests, make
the tests run in parallel, add fuzzing (randomized) tests, etc.). Remember that
you should do modifications to the test suite in another directory such as
`tests` instead of editing them directly!

## Submissions

To submit your repository, fill out [this google
form](https://docs.google.com/forms/d/e/1FAIpQLScR3vY-Cbghpvv1kZYddVVHoxoQ-Kh_j4MPoXsJv2GCkZnNMg/viewform?usp=header)
with your repository URL and your commit hash. Please ensure you are signed into
Google with your UCSC email, otherwise this form will be inaccessible. You are
welcome to resubmit up until the due date. After the due date, resubmissions are
no longer accepted. First-time submissions after the due date will count against
your grace day and may be docked credit for being late (see the syllabus for the
late policy).

Grades and feedback will be provided via git pushes to your repository via the 
zejones-asgn1-feedback branch.
