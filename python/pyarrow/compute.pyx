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
# cython: language_level = 3

from libcpp.memory cimport shared_ptr

from pyarrow.lib cimport MemoryPool, maybe_unbox_memory_pool

from pyarrow.includes.libarrow_compute cimport (
    CFunctionContext,
    Mean
)

cdef class FunctionContext:
    cdef:
        shared_ptr[CFunctionContext] ctx

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly".format(self.__class__.__name__))

    @staticmethod
    def create(MemoryPool pool):
        cdef FunctionContext self = FunctionContext.__new__(FunctionContext)
        self.ctx.reset(new CFunctionContext(maybe_unbox_memory_pool(pool)))
        return self

def mean(FunctionContext context, Array array):
    cdef CDatum datum
    check_status(Mean(context.ctx.get(), array.ap[0], &datum))
    return wrap_datum(datum)
