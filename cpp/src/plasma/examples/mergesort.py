from multiprocessing import Pool
import numpy as np
import plasma
import pyarrow as pa
import time
# import multimerge

num_cores = 32
client = None

# Connect to clients
def connect():
    global client
    client = plasma.PlasmaClient()
    client.connect("/tmp/store", "", 0)
    np.random.seed(int(time.time() * 10e7) % 10000000)

def put(array):
    tensor = pa.Tensor.from_numpy(array)
    data_size = pa.get_tensor_size(tensor)
    object_id = plasma.ObjectID(np.random.bytes(20))
    buf = client.create(object_id, data_size)
    stream = plasma.FixedSizeBufferOutputStream(buf)
    pa.write_tensor(tensor, stream)
    client.seal(object_id)
    return object_id

# def get(object_id):
#     [tensor] = client.get([object_id])
#     reader = pa.BufferReader(tensor)
#     return pa.read_tensor(reader).to_numpy()

def local_sort(object_id):
    [tensor] = client.get([object_id])
    reader = pa.BufferReader(tensor)
    array = pa.read_tensor(reader).to_numpy()
    sorted_array = np.sort(array)
    indices = np.linspace(0, len(array) - 1, num=num_cores, dtype=np.int64)
    return put(sorted_array), np.take(sorted_array, indices)

# Connect the processes in the pool
pool = Pool(initializer=connect, initargs=(), processes=num_cores)
# Connect the main process
connect()

# data = np.random.random(10000000)
data = np.random.random(200000000)
partitions = [put(array) for array in np.array_split(data, num_cores)]

object_ids, pivot_groups = list(zip(*pool.map(local_sort, partitions)))

# Choose the pivots
all_pivots = np.concatenate(pivot_groups)
indices = np.linspace(0, len(all_pivots) - 1, num=num_cores, dtype=np.int64)
pivots = np.take(np.sort(all_pivots), indices)

def local_partitions(object_id):
  [tensor] = client.get([object_id])
  reader = pa.BufferReader(tensor)
  array = pa.read_tensor(reader).to_numpy()
  split_at = array.searchsorted(pivots)
  return [put(partition) for partition in np.split(array, split_at)]

def merge(object_ids):
  tensors = []
  arrays = []
  for object_id in object_ids:
    [tensor] = client.get([object_id])
    tensors.append(tensor)
    reader = pa.BufferReader(tensor)
    array = pa.read_tensor(reader).to_numpy()
    arrays.append(array)
  return put(multimerge.multimerge(*arrays))

pool = Pool(initializer=connect, initargs=(), processes=num_cores)
results = list(zip(*pool.map(local_partitions, object_ids)))
