#include "plasma_table.h"

#include "thirdparty/shm_malloc.h"

#define uthash_malloc(sz) shm_malloc(sz)
#define uthash_free(ptr,sz) shm_free(ptr)

#include "thirdparty/uthash.h"

#include "arrow/util/logging.h"

namespace plasma {

struct PlasmaTableEntry {
    ObjectID id;
    int64_t data_size;
    int64_t metadata_size;
    uint8_t* pointer;
    // For locking the following condition variable.
    pthread_mutex_t mutex;
    // This will signal to other processes that the object is available.
    pthread_cond_t cond;
    // Handle for putting this entry into the hash table.
    UT_hash_handle hh;
};

PlasmaTableEntry* PlasmaTable::MakePlasmaTableEntry(const ObjectID& id, int64_t data_size, int64_t metadata_size, uint8_t* pointer) {
  auto entry = reinterpret_cast<PlasmaTableEntry*>(shm_calloc(1, sizeof(PlasmaTableEntry)));
  entry->id = id;
  entry->data_size = data_size;
  entry->metadata_size = metadata_size;
  entry->pointer = pointer;
  pthread_mutex_init(&entry->mutex, &mutex_attr_);
  pthread_cond_init(&entry->cond, &cond_attr_);
  return entry;
}

Status PlasmaTable::Init() {
  pthread_mutexattr_init(&mutex_attr_);
  pthread_mutexattr_setpshared(&mutex_attr_, PTHREAD_PROCESS_SHARED);

  pthread_condattr_init(&cond_attr_);
  pthread_condattr_setpshared(&cond_attr_, PTHREAD_PROCESS_SHARED);

  pthread_rwlockattr_init(&rwlock_attr_);
  pthread_rwlockattr_setpshared(&rwlock_attr_, PTHREAD_PROCESS_SHARED);

  ARROW_CHECK(pthread_rwlock_init(&lock_, &rwlock_attr_) == 0);

  return Status::OK();
}

PlasmaTable* MakeSharedPlasmaTable() {
  PlasmaTable *table = reinterpret_cast<PlasmaTable*>(shm_malloc(sizeof(PlasmaTable)));
  ARROW_CHECK_OK(table->Init());
  return table;
}

Status PlasmaTable::Lookup(const ObjectID& id, int64_t* data_size, int64_t* metadata_size, uint8_t** pointer) {
  PlasmaTableEntry* entry;
  ARROW_CHECK(pthread_rwlock_rdlock(&lock_) == 0);
  HASH_FIND(hh, table_, &id, sizeof(id), entry);
  pthread_rwlock_unlock(&lock_);
  if (entry) {
    *data_size = entry->data_size;
    *metadata_size = entry->metadata_size;
    *pointer = entry->pointer;
  } else {
    *data_size = -1;
    *metadata_size = -1;
    *pointer = nullptr;
  }
  return Status::OK();
}

Status PlasmaTable::Add(const ObjectID& id, int64_t data_size, int64_t metadata_size, uint8_t* pointer) {
  PlasmaTableEntry* entry;
  ARROW_CHECK(pthread_rwlock_rdlock(&lock_) == 0);
  HASH_FIND(hh, table_, &id, sizeof(id), entry);
  pthread_rwlock_unlock(&lock_);
  if (entry && entry->data_size != -1) {
    // The object already exists in the object store.
    return Status::PlasmaObjectExists("object already exists in the plasma store");
  } else if (entry) {
    // The object doesn't exist in the object store yet, but there is at
    // least one Get waiting for it.
    pthread_mutex_lock(&entry->mutex);
    entry->data_size = data_size;
    entry->metadata_size = metadata_size;
    entry->pointer = pointer;
    pthread_cond_signal(&entry->cond);
    pthread_mutex_unlock(&entry->mutex);
  } else {
    // The object doesn't exist in the object store yet and we need to
    // create an entry for it in the object table.
    entry = MakePlasmaTableEntry(id, data_size, metadata_size, pointer);
    ARROW_CHECK(pthread_rwlock_wrlock(&lock_) == 0);
    HASH_ADD(hh, table_, id, sizeof(id), entry);
    pthread_rwlock_unlock(&lock_);
  }
  return Status::OK();
}

// It is important that we do not hold the read-write lock
// while we are blocked in Get so other clients can put the object
// into the table.
Status PlasmaTable::Get(const ObjectID& id, int64_t* data_size, int64_t* metadata_size, uint8_t** pointer, int64_t deadline) {
  RETURN_NOT_OK(Lookup(id, data_size, metadata_size, pointer));
  if (!*pointer) {
    PlasmaTableEntry* entry = MakePlasmaTableEntry(id, -1, -1, nullptr);
    ARROW_CHECK(pthread_rwlock_wrlock(&lock_) == 0);
    HASH_ADD(hh, table_, id, sizeof(id), entry);
    pthread_rwlock_unlock(&lock_);
    timespec ts;
    ts.tv_sec = deadline / 1000;
    ts.tv_nsec = (deadline % 1000) * 1000 * 1000;
    pthread_mutex_lock(&entry->mutex);
    while (!entry->pointer) {
      int r = pthread_cond_timedwait(&entry->cond, &entry->mutex, &ts);
      if (r == ETIMEDOUT) {
        *data_size = -1;
        *metadata_size = -1;
        *pointer = nullptr;
        pthread_mutex_unlock(&entry->mutex);
        return Status::OK();
      }
    }
    *data_size = entry->data_size;
    *metadata_size = entry->metadata_size;
    *pointer = entry->pointer;
    pthread_mutex_unlock(&entry->mutex);
  }
  return Status::OK();
}

}  // namespace plasma
