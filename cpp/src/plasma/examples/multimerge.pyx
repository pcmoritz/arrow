# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True

from libc.stdint cimport uintptr_t
from libcpp.vector cimport vector
from libcpp.pair cimport pair

cimport numpy as np
import numpy as np

cdef extern from "<queue>" namespace "std" nogil:
    cdef cppclass priority_queue[T]:
        priority_queue() except +
        priority_queue(priority_queue&) except +
        #priority_queue(Container&)
        bint empty()
        void pop()
        void push(T&)
        size_t size()
        T& top()

def multimerge(*arrays):
  cdef int num_arrays = len(arrays)
  cdef vector[double*] data
  cdef vector[int] indices = num_arrays * [0]
  cdef vector[int] sizes
  cdef priority_queue[pair[double, int]] queue
  cdef pair[double, int] top
  cdef int num_elts = sum([len(array) for array in arrays])
  cdef np.ndarray[np.float64_t, ndim=1] result = np.zeros(num_elts, dtype=np.float64)
  cdef double* result_ptr = <double*> np.PyArray_DATA(result)
  cdef int curr_idx = 0
  for i in range(num_arrays):
    sizes.push_back(len(arrays[i]))
    data.push_back(<double*> np.PyArray_DATA(arrays[i]))
    queue.push(pair[double, int](-data[i][0], i))
  while curr_idx < num_elts:
    top = queue.top()
    result_ptr[curr_idx] = data[top.second][indices[top.second]]
    indices[top.second] += 1
    queue.pop()
    if indices[top.second] < sizes[top.second]:
      queue.push(pair[double, int](-data[top.second][indices[top.second]], top.second))
    curr_idx += 1
  return result
