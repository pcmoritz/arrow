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
  PlasmaTableEntry* MakePlasmaTableEntry(const ObjectID& id, int64_t data_size, int64_t metadata_size, uint8_t* pointer);
  Status Lookup(const ObjectID& id, int64_t* data_size, int64_t* metadata_size, int64_t* reference_count, int64_t* lru_time, uint8_t** pointer);
  Status Add(const ObjectID& id, int64_t data_size, int64_t metadata_size, uint8_t* pointer);
  // Get an item and block if it is not there.
  Status Get(const ObjectID& id, int64_t* data_size, int64_t* metadata_size, uint8_t** pointer, int64_t deadline);
  Status Delete(const ObjectID& id);
  Status GetRandomElement(ObjectID* id);
  Status WaitForNotification();
  Status GetNotification(int fd, ObjectID* object_id,
                         int64_t* data_size, int64_t* metadata_size);
  Status IncrementReferenceCount(const ObjectID& id);
  Status DecrementReferenceCount(const ObjectID& id);

private:
  pthread_condattr_t cond_attr_;
  pthread_mutexattr_t mutex_attr_;
  pthread_rwlockattr_t rwlock_attr_;
  pthread_rwlock_t lock_;
  // For locking the following condition variable.
  pthread_mutex_t notification_mutex_;
  // This will signal to other processes that a new object is available.
  pthread_cond_t notification_cond_;
  int64_t num_notifications_;
  // Total number of bytes in the store.
  int64_t allocated_bytes_;
  PlasmaTableEntry* table_;
};

PlasmaTable* MakeSharedPlasmaTable();

};  // namespace plasma

#endif  // PLASMA_TABLE
