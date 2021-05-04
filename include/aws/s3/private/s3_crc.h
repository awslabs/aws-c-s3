#ifndef AWS_S3_CRC_H
#define AWS_S3_CRC_H

#define AWS_CRC_LEN 4

struct aws_hash *aws_hash_crc32_new(struct aws_allocator *allocator);

struct aws_hash *aws_hash_crc32c_new(struct aws_allocator *allocator);

#endif /* AWS_S3_CRC_H */
