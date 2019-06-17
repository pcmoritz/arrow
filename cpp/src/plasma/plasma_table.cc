#include "plasma/plasma_table.h"

#include <chrono>

#include "plasma/thirdparty/shm_malloc.h"

#define uthash_malloc(sz) shm_malloc(sz)
#define uthash_free(ptr,sz) shm_free(ptr)

#include "plasma/thirdparty/uthash.h"

#include "arrow/util/logging.h"

namespace plasma {

struct PlasmaTableEntry {
    ObjectID id;
    int64_t data_size;
    int64_t metadata_size;
    uint8_t* pointer;
    int64_t reference_count;
    int64_t lru_time;
    // For locking the following condition variable.
    pthread_mutex_t mutex;
    // This will signal to other processes that the object is available.
    pthread_cond_t cond;
    // Handle for putting this entry into the hash table.
    UT_hash_handle hh;
};

int64_t GetTimeMs() {
  auto now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
}

PlasmaTableEntry* PlasmaTable::MakePlasmaTableEntry(const ObjectID& id, int64_t data_size, int64_t metadata_size, uint8_t* pointer) {
  auto entry = reinterpret_cast<PlasmaTableEntry*>(shm_calloc(1, sizeof(PlasmaTableEntry)));
  entry->id = id;
  entry->data_size = data_size;
  entry->metadata_size = metadata_size;
  entry->pointer = pointer;
  entry->reference_count = 1;
  entry->lru_time = GetTimeMs();
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

  pthread_mutex_init(&notification_mutex_, &mutex_attr_);
  pthread_cond_init(&notification_cond_, &cond_attr_);

  num_notifications_ = 0;

  return Status::OK();
}

PlasmaTable* MakeSharedPlasmaTable() {
  PlasmaTable *table = reinterpret_cast<PlasmaTable*>(shm_malloc(sizeof(PlasmaTable)));
  ARROW_CHECK_OK(table->Init());
  return table;
}

Status PlasmaTable::Lookup(const ObjectID& id, int64_t* data_size, int64_t* metadata_size, int64_t* reference_count, int64_t* lru_time, uint8_t** pointer) {
  // TODO: Update LRU time
  PlasmaTableEntry* entry;
  ARROW_CHECK(pthread_rwlock_rdlock(&lock_) == 0);
  HASH_FIND(hh, table_, &id, sizeof(id), entry);
  pthread_rwlock_unlock(&lock_);
  if (entry) {
    *data_size = entry->data_size;
    *metadata_size = entry->metadata_size;
    *pointer = entry->pointer;
    *reference_count = entry->reference_count;
    *lru_time = entry->lru_time;
  } else {
    *data_size = -1;
    *metadata_size = -1;
    *pointer = nullptr;
    *reference_count = -1;
    *lru_time = 0;
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
    entry->reference_count = 1;
    entry->lru_time = GetTimeMs();
    // Need to re-enter the entry into the object table so
    // that the hashtable linked list is extended and the right
    // object notification will be triggered.
    HASH_DEL(table_, entry);
    HASH_ADD(hh, table_, id, sizeof(id), entry);
    pthread_cond_signal(&entry->cond);
    pthread_mutex_unlock(&entry->mutex);

    pthread_mutex_lock(&notification_mutex_);
    num_notifications_ += 1;
    pthread_cond_signal(&notification_cond_);
    pthread_mutex_unlock(&notification_mutex_);
  } else {
    // The object doesn't exist in the object store yet and we need to
    // create an entry for it in the object table.
    entry = MakePlasmaTableEntry(id, data_size, metadata_size, pointer);
    ARROW_CHECK(pthread_rwlock_wrlock(&lock_) == 0);
    HASH_ADD(hh, table_, id, sizeof(id), entry);
    allocated_bytes_ += (data_size + metadata_size);
    pthread_rwlock_unlock(&lock_);

    pthread_mutex_lock(&notification_mutex_);
    num_notifications_ += 1;
    pthread_cond_signal(&notification_cond_);
    pthread_mutex_unlock(&notification_mutex_);
  }
  return Status::OK();
}

// It is important that we do not hold the read-write lock
// while we are blocked in Get so other clients can put the object
// into the table.
Status PlasmaTable::Get(const ObjectID& id, int64_t* data_size, int64_t* metadata_size, uint8_t** pointer, int64_t deadline) {
  int64_t reference_count;
  int64_t lru_time;
  RETURN_NOT_OK(Lookup(id, data_size, metadata_size, &reference_count, &lru_time, pointer));
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

Status PlasmaTable::Delete(const ObjectID& id) {
  PlasmaTableEntry* entry;
  ARROW_CHECK(pthread_rwlock_wrlock(&lock_) == 0);
  HASH_FIND(hh, table_, &id, sizeof(id), entry);
  if (entry) {
    HASH_DEL(table_, entry);
  }
  pthread_rwlock_unlock(&lock_);
  shm_free(entry);
  return Status::OK();
}

Status PlasmaTable::GetRandomElement(ObjectID* id) {
  ARROW_CHECK(pthread_rwlock_rdlock(&lock_) == 0);
  unsigned long num_buckets = table_->hh.tbl->num_buckets;
  ARROW_CHECK(num_buckets > 0);
  unsigned long bucket = random() % num_buckets;
  UT_hash_handle* handle = nullptr;
  for (unsigned long i = 0; i < num_buckets; ++i) {
    handle = table_->hh.tbl->buckets[(bucket + i) % num_buckets].hh_head;
    if (handle) break;
  }
  if (!handle) {
    return Status::PlasmaObjectNonexistent("");
  }
  // First we count the number of buckets
  UT_hash_handle* h = handle;
  unsigned long bucket_length = 0;
  while (h) {
    h = h->hh_next;
    bucket_length++;
  }
  // Now we find a random element in the bucket
  ARROW_CHECK(bucket_length > 0);
  unsigned long bucket_index = random() % bucket_length;
  h = handle;
  while (bucket_index--) {
    h = h->hh_next;
  }
  ARROW_CHECK(h->keylen == sizeof(ObjectID));
  memcpy(id->mutable_data(), h->key, h->keylen);
  pthread_rwlock_unlock(&lock_);
  return Status::OK();
}

Status PlasmaTable::WaitForNotification() {
  static int64_t num_notifications = 0;
  pthread_mutex_lock(&notification_mutex_);
  // TODO: Need to block here!
  while (num_notifications >= num_notifications_) {
    std::cout << "next notification " << num_notifications << " " << num_notifications_ << std::endl;
    int r = pthread_cond_wait(&notification_cond_, &notification_mutex_);
  }
  num_notifications += 1;
  pthread_mutex_unlock(&notification_mutex_);
  return Status::OK();
}

Status PlasmaTable::GetNotification(int fd, ObjectID* object_id,
                                    int64_t* data_size, int64_t* metadata_size) {
  static bool first_iteration = true;
  static PlasmaTableEntry* entry = table_;
  ARROW_CHECK(pthread_rwlock_rdlock(&lock_) == 0);
  if (first_iteration) {
    *object_id = entry->id;
    *data_size = entry->data_size;
    *metadata_size = entry->metadata_size;
    first_iteration = false;
  }
  if (entry->hh.next && !first_iteration) {
    entry = reinterpret_cast<PlasmaTableEntry*>(entry->hh.next);
    *object_id = entry->id;
    *data_size = entry->data_size;
    *metadata_size = entry->metadata_size;
  }
  pthread_rwlock_unlock(&lock_);
  return Status::OK();
}

Status PlasmaTable::IncrementReferenceCount(const ObjectID& id) {
  PlasmaTableEntry* entry;
  // TODO(pcm): Change this to a rdlock and use atomics for the reference_count
  ARROW_CHECK(pthread_rwlock_wrlock(&lock_) == 0);
  HASH_FIND(hh, table_, &id, sizeof(id), entry);
  if (entry) {
    entry->reference_count += 1;
  }
  pthread_rwlock_unlock(&lock_);
  return Status::OK();
}

Status PlasmaTable::DecrementReferenceCount(const ObjectID& id) {
  PlasmaTableEntry* entry;
  // TODO(pcm): Change this to a rdlock and use atomics for the reference_count
  ARROW_CHECK(pthread_rwlock_wrlock(&lock_) == 0);
  HASH_FIND(hh, table_, &id, sizeof(id), entry);
  if (entry) {
    entry->reference_count -= 1;
  }
  pthread_rwlock_unlock(&lock_);
  return Status::OK();
}

}  // namespace plasma
