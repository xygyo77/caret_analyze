#!/usr/bin/env /usr/bin/python3
# -*- coding: utf-8 -*-

### for debug
import inspect
import os

def location(depth=0):
    """Get execution location (file name, function name & line number)

    Args:
        depth (int, optional): Depth of the caller in the call stack. Defaults to 0.

    Returns:
        tuple: File name, function name, and line number of the caller.
    """
    frame = inspect.currentframe().f_back
    for _ in range(depth):
        frame = frame.f_back
    return os.path.basename(frame.f_code.co_filename), frame.f_code.co_name, frame.f_lineno

def D(x=0, d=1):
    pre = "!"
    for n in range(d):
        fn, func, ln = location(depth=n+1)
        if pre != "!":
            x=""
        print(f"{pre} {func}: {ln} {x} ::{fn}")
        pre += '!'