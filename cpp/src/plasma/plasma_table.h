#ifndef PLASMA_TABLE
#define PLASMA_TABLE

#include <pthread.h>

#include "plasma/common.h"

using arrow::Status;

namespace plasma {

struct PlasmaTableEntry;

/// A hash table that lives in shared memory
class PlasmaTable {
public:
  Status Init();
  PlasmaTableEntry* MakePlasmaTableEntry(const ObjectID& id, int64_t data_size, uint8_t* pointer);
  Status Lookup(const ObjectID& id, int64_t* data_size, uint8_t** pointer);
  Status Add(const ObjectID& id, int64_t data_size, uint8_t* pointer);
  // Get an item and block if it is not there.
  Status Get(const ObjectID& id, int64_t* data_size, uint8_t** pointer);

private:
  pthread_condattr_t cond_attr_;
  pthread_mutexattr_t mutex_attr_;
  pthread_rwlockattr_t rwlock_attr_;
  pthread_rwlock_t lock_;
  PlasmaTableEntry* table_;
};

PlasmaTable* MakeSharedPlasmaTable();

};  // namespace plasma

#endif  // PLASMA_TABLE
