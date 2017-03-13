// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef ARROW_UTIL_MEM_UTIL_H
#define ARROW_UTIL_MEM_UTIL_H
#include <cassert>
#include <vector>
#include <thread>

#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>

namespace arrow {

static inline int memcopy_aligned(uint8_t *dst, const uint8_t *src, uint64_t nbytes, bool timeit)
{
#ifndef NUMTHREADS
#define NUMTHREADS 8
#endif
  int rv = 0;
  struct timeval tv1, tv2;
  double elapsed = 0;
  const uint64_t numthreads = NUMTHREADS;
  const uint64_t blocksz = getpagesize();
  const char *srcbp = (char *)(((uint64_t)src + blocksz-1) & ~(blocksz-1));
  char *srcep = (char *)(((uint64_t)src + nbytes) & ~(blocksz-1));

  const uint64_t numblocks = (((uint64_t)srcep - (uint64_t)srcbp)) / blocksz;
  // Now we divide these blocks between available threads. Remainder is pushed
  // to the suffix-handling thread.
  // uint64_t remainder = numblocks % numthreads;
  // Update the end pointer
  srcep = srcep - (numblocks % numthreads)*blocksz;
  const uint64_t chunksz = ((uint64_t)srcep - (uint64_t)srcbp) / numthreads;//B
  //assert(srcep >= srcbp);
  const uint64_t prefix = (uint64_t)srcbp - (uint64_t)src; // Bytes
  const uint64_t suffix = (uint64_t)(src+nbytes) - (uint64_t)srcep; // Bytes
  char *dstep = (char *)((uint64_t)dst + prefix + numthreads*chunksz);
  // Now data == | prefix | k*numthreads*blocksz | suffix |
  // chunksz = k*blocksz => data == | prefix | numthreads*chunksz | suffix |
  // Each thread gets a "chunk" of k blocks, except prefix and suffix threads.

  std::vector<std::thread> threads;
  // Start the prefix thread.
  if (timeit) {
    gettimeofday(&tv1, NULL);
  }
  threads.push_back(std::thread(memcpy, dst, src, prefix));
  for (int i = 1; i <= numthreads; i++) {
    threads.push_back(std::thread(
        memcpy, dst+prefix+(i-1)*chunksz, srcbp + (i-1)*chunksz, chunksz));
  }
  threads.push_back(std::thread(memcpy, dstep, srcep, suffix));

  // Join the memcpy threads.
  for (auto &t: threads) {
    t.join();
  }
  if (timeit) {
    gettimeofday(&tv2, NULL);
    elapsed = ((tv2.tv_sec - tv1.tv_sec)*1000000 + (tv2.tv_usec - tv1.tv_usec))/1000000.0;
    printf("copied %llu bytes in time = %8.4f MBps=%8.4f\n",
           nbytes, elapsed, nbytes/((1<<20)*elapsed));
  }
  return rv;
}

static inline int memset_aligned(uint8_t *dst, int val, uint64_t nbytes, bool timeit)
{
#ifndef NUMTHREADS
#define NUMTHREADS 8
#endif
  int rv = 0;
  struct timeval tv1, tv2;
  double elapsed = 0;
  const uint64_t numthreads = NUMTHREADS;
  const uint64_t blocksz = 64; // cache block aligned
  const char *dstbp = (char *)(((uint64_t)dst + blocksz-1) & ~(blocksz-1));
  char *dstep = (char *)(((uint64_t)dst + nbytes) & ~(blocksz-1));
  const uint64_t chunksz = ((uint64_t)dstep - (uint64_t)dstbp) / numthreads;//B
  //assert(dstep >= dstbp);
  const uint64_t numblocks = (((uint64_t)dstep - (uint64_t)dstbp)) / blocksz;
  // Now we divide these blocks between available threads. Remainder is pushed
  // to the suffix-handling thread.
  // uint64_t remainder = numblocks % numthreads;
  // Update the end pointer
  dstep = dstep - (numblocks % numthreads)*blocksz;
  const uint64_t prefix = (uint64_t)dstbp - (uint64_t)dst; // Bytes
  const uint64_t suffix = (uint64_t)(dst+nbytes) - (uint64_t)dstep; // Bytes
  std::vector<std::thread> threads;
  // Start the prefix thread.
  if (timeit) {
    gettimeofday(&tv1, NULL);
  }
  threads.push_back(std::thread(memset, dst, val, prefix));
  for (int i = 1; i <= numthreads; i++) {
    threads.push_back(std::thread(
        memset, dst+prefix+(i-1)*chunksz, val, chunksz));
  }
  threads.push_back(std::thread(memset, dstep, val, suffix));

  // Join the memcpy threads.
  for (auto &t: threads) {
    t.join();
  }
  if (timeit) {
    gettimeofday(&tv2, NULL);
    elapsed = ((tv2.tv_sec - tv1.tv_sec)*1000000 + (tv2.tv_usec - tv1.tv_usec))/1000000.0;
    printf("copied %llu bytes in time = %8.4f MBps=%8.4f\n",
           nbytes, elapsed, nbytes/((1<<20)*elapsed));
  }
  return rv;
}

}  // namespace arrow

#endif  // ARROW_UTIL_MEM_UTIL_H
