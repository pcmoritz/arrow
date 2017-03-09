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
#if 0
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>
#endif

#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>

namespace arrow {

static inline int memcopy_frame_aligned(uint8_t *dst, const uint8_t *src, uint64_t nbytes, bool runparallel) {
  struct timeval tv1, tv2;
  double elapsed = 0;
  // assume src and dst are ready to go (allocated, populated, etc)
  //printf("src=%p\tdst=%p\n", src, dst); 
  int rv = 0;
  int pagesz = getpagesize();
  char *srcbp = (char *)(((uint64_t)src + 4095) & ~(0x0fff));
  char *srcep = (char *)(((uint64_t)src + nbytes) & ~(0x0fff));
  uint64_t prefix = (uint64_t)srcbp - (uint64_t)src;
  uint64_t suffix = ((uint64_t)src + nbytes) % 4096;
  uint64_t numpages = (nbytes-prefix)/pagesz;
  char *dstep = (char *)((uint64_t)dst + prefix + numpages*pagesz);

  //gettimeofday(&tv1, NULL);
  memcpy(dst, src, prefix);
  #pragma omp parallel for num_threads(8) if (runparallel)
  for (uint64_t i = 0; i < numpages; i++)
  {
    memcpy((char *)(dst) + prefix + i*pagesz, ((char *)srcbp) + i*pagesz, pagesz);
  }
  memcpy(dstep, srcep, suffix);
  //gettimeofday(&tv2, NULL);
  //elapsed = ((tv2.tv_sec - tv1.tv_sec)*1000000 + (tv2.tv_usec - tv1.tv_usec))/1000000.0;
  //printf("copied %ld bytes in time = %8.4f MBps=%8.4f\n", nbytes, elapsed, nbytes/((1<<20)*elapsed));
  return rv; // 0 is good; bad o.w.
}


}  // namespace arrow

#endif  // ARROW_UTIL_MEM_UTIL_H
