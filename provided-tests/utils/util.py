import subprocess
import sys
from collections.abc import Collection
from dataclasses import dataclass
from typing import Optional, TextIO


@dataclass
class Logger:
    files: Collection[TextIO]
    prefix: Optional[str] = None

    def __call__(self, *args):
        if self.prefix is not None:
            prefix = (f"{self.prefix}: ",)
        else:
            prefix = ()
        for file in self.files:
            print(*prefix, *args, file=file)


_GLOBAL_LOGGER = Logger(prefix=None, files=(sys.stderr,))


def log(*args):
    _GLOBAL_LOGGER(*args)


def global_logger() -> Logger:
    return _GLOBAL_LOGGER


def run_cmd_bg(
    cmd: list[str],
    log: Logger = _GLOBAL_LOGGER,
    verbose=False,
    error_prefix: str = "command failed",
    **kwargs,
) -> subprocess.CompletedProcess:
    # default capture opts
    kwargs.setdefault("stdout", subprocess.PIPE)
    kwargs.setdefault("stderr", subprocess.PIPE)
    kwargs.setdefault("text", True)
    kwargs.setdefault("check", True)

    if verbose:
        log(f"$ {cmd[0]} {' '.join(cmd[1:])}")

    try:
        return subprocess.run(cmd, **kwargs)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"{error_prefix}: {e}:\nstdout: {e.stdout}\nstderr: {e.stderr}"
        )
