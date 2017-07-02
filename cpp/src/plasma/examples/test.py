import multimerge
import numpy as np

arrays = [np.sort(np.random.randn(1000000)) for i in range(20)]

result = multimerge.multimerge(*arrays)

print("res", result)
print("sorted", np.sort(np.concatenate(arrays)))

import IPython
IPython.embed()

assert (result == np.sort(np.concatenate(arrays))).all()
