#include "common.h"

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "io.h"

/* This is used to define the array of object IDs. */
const UT_icd object_id_icd = {sizeof(ObjectID), NULL, NULL, NULL};

const UniqueID NIL_ID = {{255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                          255, 255, 255, 255, 255, 255, 255, 255, 255, 255}};

const unsigned char NIL_DIGEST[DIGEST_SIZE] = {0};

UniqueID globally_unique_id(void) {
  /* Use /dev/urandom for "real" randomness. */
  int fd;
  int const flags = 0 /* for Windows compatibility */;
  if ((fd = open("/dev/urandom", O_RDONLY, flags)) == -1) {
    ARROW_LOG(ERROR) << "Could not generate random number";
  }
  UniqueID result;
  if(read_bytes(fd, &result.id[0], UNIQUE_ID_SIZE) < 0) {
    ARROW_LOG(FATAL) << "read_bytes failed";
  }
  close(fd);
  return result;
}

bool ObjectID_equal(ObjectID first_id, ObjectID second_id) {
  return UNIQUE_ID_EQ(first_id, second_id);
}

bool ObjectID_is_nil(ObjectID id) {
  return ObjectID_equal(id, NIL_OBJECT_ID);
}

char *ObjectID_to_string(ObjectID obj_id, char *id_string, int id_length) {
  DCHECK(id_length >= ID_STRING_SIZE);
  static const char hex[] = "0123456789abcdef";
  char *buf = id_string;

  for (int i = 0; i < UNIQUE_ID_SIZE; i++) {
    unsigned int val = obj_id.id[i];
    *buf++ = hex[val >> 4];
    *buf++ = hex[val & 0xf];
  }
  *buf = '\0';

  return id_string;
}
