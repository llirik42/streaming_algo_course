from itertools import cycle

import matplotlib.pyplot as plt
import json
import numpy as np

s = """PLACE OUTPUT HERE"""

data = {
    "probability": [],
    "duration": [],
    "alloc": [],
    "totalAlloc": [],
    "sys": [],
    "heapAlloc": [],
    "stackSys": [],
}

for line in s.split("\n"):
    current_data = json.loads(line)
    for key in data.keys():
        data[key].append(current_data[key])

x = data["probability"]
cyccle = cycle('bgrcmk')
plt.rcParams.update({"font.size": 14})
for k in data.keys():
    if k == "probability":
        continue

    plt.figure()
    plt.plot(x, data[k], linewidth=2.5, c=cyccle.__next__())
    plt.xlabel("Probability")
    plt.ylabel(k)

plt.show()
