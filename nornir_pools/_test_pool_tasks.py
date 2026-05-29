"""Importable worker callables for nornir-pools test multiprocessing."""

from __future__ import annotations

import os
import random
import time

FILENAME_TEMPLATE = "%04d.txt"


def CreateFile(path: str, number: int) -> None:
    """Create ``<number>.txt`` in ``path`` containing ``number``."""
    filename = FILENAME_TEMPLATE % number
    filenamefullpath = os.path.join(path, filename)
    with open(filenamefullpath, "w+", encoding="utf-8") as hFile:
        hFile.write(str(number))


def ReadFile(path: str, number: int) -> int:
    filename = FILENAME_TEMPLATE % number
    filenamefullpath = os.path.join(path, filename)
    with open(filenamefullpath, "r", encoding="utf-8") as hFile:
        return int(hFile.read())


def SleepForRandomTime(MaxTime: float = 0.25) -> int:
    """Sleep for a random duration and return worker pid."""
    sleepTime = random.random() * MaxTime
    time.sleep(sleepTime)
    return os.getpid()


def CreateFileWithDelay(path: str, number: int) -> None:
    SleepForRandomTime()
    CreateFile(path, number)


def ReadFileWithDelay(path: str, number: int) -> int:
    SleepForRandomTime()
    return ReadFile(path, number)


class IntentionalPoolException(Exception):
    pass


def RaiseException(msg: str | None = None) -> None:
    if msg is None:
        msg = ""
    raise IntentionalPoolException(msg)


def SquareTheNumberWithDelay(num: float) -> float:
    SleepForRandomTime()
    return SquareTheNumber(num)


def SquareTheNumber(num: float) -> float:
    return num * num
