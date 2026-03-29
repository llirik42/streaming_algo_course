#!/usr/bin/python3

import sys
import json

data = {}

for i, line in enumerate(sys.stdin):
    line = line.strip()

    if i == 2:
        data["probability"] = float(line)
    if i == 3:
        data["duration"] = float(line[:-1])  # remove "s" (seconds)
    if i == 4:
        data["alloc"] = int(line)
    if i == 5:
        data["totalAlloc"] = int(line)
    if i == 6:
        data["sys"] = int(line)
    if i == 7:
        data["heapAlloc"] = int(line)
    if i == 8:
        data["stackSys"] = int(line)

print(json.dumps(data))
