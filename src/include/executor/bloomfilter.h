#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H
#include <stdint.h>
#include <emmintrin.h>
#include "c.h"

/*
 * The BloomFilter is divided up into Buckets
 * Each bucket is 8 X sizeof(uint32_t) bytes, 256 bits.
 */
#define BUCKET_WORDS 8
typedef uint32_t BucketWord;
typedef BucketWord Bucket[BUCKET_WORDS];

// log2(number of bits in a BucketWord)
#define LOG_BUCKET_WORD_BITS 5
#define BUCKET_WORD_MASK ((1 << LOG_BUCKET_WORD_BITS) - 1)

// log2(number of bytes in a bucket)
#define LOG_BUCKET_BYTE_SIZE = 5

uint32_t REHASH[8] __attribute__((aligned(32))) = { 0x47b6137bU, 0x44974d91U,
        0x8824ad5bU, 0xa2b7289dU, 0x705495c7U, 0x2df1424bU, 0x9efc4947U,
        0x5c6bfb31U };

typedef struct BloomFilter
{
    bool     always_false;
    int      log_num_buckets;
    uint32_t directory_mask;
    Bucket*  directory;
} BloomFilter;

extern void BloomFilterInit(BloomFilter *bf, int log_heap_size);
extern void BloomFilterInsert(BloomFilter *bf, uint32_t hash);
extern bool BloomFilterFind(BloomFilter *bf, uint32_t hash);
extern void BloomFilterPrint(BloomFilter *bf);
extern double BloomFilterGetFPP(BloomFilter *bf, size_t ndv, int log_heap_space);
extern void BloomFilterDestroy(BloomFilter *bf);

#endif
