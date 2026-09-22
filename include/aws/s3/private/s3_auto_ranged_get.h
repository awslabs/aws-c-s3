#ifndef AWS_S3_AUTO_RANGED_GET_H
#define AWS_S3_AUTO_RANGED_GET_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_meta_request_impl.h"

enum aws_s3_auto_ranged_get_request_type {
    AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_HEAD_OBJECT,
    AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_GET_OBJECT_WITH_RANGE,
    AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_GET_OBJECT_WITH_PART_NUMBER_1,
};

struct aws_s3_auto_ranged_get {
    struct aws_s3_meta_request base;

    enum aws_s3_checksum_algorithm validation_algorithm;

    struct aws_string *etag;

    /* S3 Last-Modified header value, captured from discovery response for resume token */
    struct aws_string *s3_object_last_modified;

    /* Estimated object stored part size based on ETag analysis */
    uint64_t estimated_object_stored_part_size;
    /* Number of parts stored in S3. We derive this from ETag, if ETag is not formatted as expected, this will be
     * default to 1.
     * Note: For S3Express Append, the object will be treated as a single part, even though, it can be multiple parts
     * stored in S3.
     */
    uint64_t num_stored_parts;
    /* Part size was set or not from user for this meta request. */
    bool part_size_set;
    bool force_dynamic_part_size;

    bool initial_message_has_start_range;
    bool initial_message_has_end_range;
    uint64_t initial_range_start;
    uint64_t initial_range_end;

    uint64_t object_size_hint;
    bool object_size_hint_available;

    /* Members to only be used when the mutex in the base type is locked. */
    struct {
        /* The starting byte of the data that we will be retrieved from the object.
         * (ignore this if object_range_empty) */
        uint64_t object_range_start;

        /* The last byte of the data that will be retrieved from the object.
         * (ignore this if object_range_empty)
         * Note this is inclusive: https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests
         * So if begin=0 and end=0 then 1 byte is being downloaded. */
        uint64_t object_range_end;

        /* Full size of the S3 object, from discovery (the Content-Range total, or Content-Length
         * when the request had no Range header). May be larger than the range being downloaded. */
        uint64_t object_size;

        uint64_t first_part_size;

        /* The total number of parts that are being used in downloading the object range. Note that "part" here
         * currently refers to a range-get, and does not require a "part" on the service side. */
        uint32_t total_num_parts;

        uint32_t num_parts_requested;
        uint32_t num_parts_completed;
        uint32_t num_parts_successful;
        uint32_t num_parts_failed;
        uint32_t num_parts_checksum_validated;

        /* Block-cyclic spread. A download normally sweeps the object from part 1 to part N, so the
         * requests in flight at any moment cover one contiguous span of it. Spreading instead cuts the
         * parts still to be requested into `spread_num_regions` contiguous regions and rotates between
         * them, so the requests in flight sit in that many far-apart regions of the object.
         *
         * Say 10 parts, 2 through 11, are to be spread across 3 regions. 10 divided by 3 is 3 with 1
         * left over, so the leftover part has to go somewhere: one region holds 4 parts and the other
         * two hold 3, which cuts the object up as
         *
         *     region 0   region 1  region 2
         *     [2 3 4 5]  [6 7 8]   [9 10 11]
         *
         * That division is what the first three fields hold -- `spread_num_regions` is 3,
         * `spread_region_size` is 3, `spread_num_large_regions` is 1 -- and `spread_first_part` is 2.
         * The handout then rotates one part from each region in turn: 2, 6, 9, 3, 7, 10, 4, 8, 11, 5.
         * So the 3 requests in flight at any moment are in 3 different regions of the object, and each
         * region is still fetched front to back.
         *
         * These five fields are the entire state. `s_init_spread_synced` fills in the four constants and
         * `s_next_part_number_synced` derives each part number from `spread_parts_handed_out` and those
         * constants, so no per-region cursor is kept. */

        /* Number of regions the parts are spread across, and the on/off switch for the whole feature:
         * 0 means hand parts out in object order and ignore every field below. 3 in the example above. */
        uint32_t spread_num_regions;

        /* How many parts a region holds, floor(parts_to_spread / spread_num_regions) -- 3 in the
         * example above. Every region holds this many parts, except the `spread_num_large_regions`
         * that hold one more. Doubles as the step from one region's first part to the next region's,
         * once those larger regions are behind you. */
        uint32_t spread_region_size;

        /* How many regions hold one extra part, parts_to_spread % spread_num_regions -- 1 in the
         * example above, which is why region 0 holds 4 parts and regions 1 and 2 hold 3. It is always
         * the first this-many regions that are the larger ones. 0 when the parts divide evenly. */
        uint32_t spread_num_large_regions;

        /* 1-based part number the first region begins at, so the spread covers
         * [spread_first_part, total_num_parts] -- 2 in the example above. Not always 1: a ranged-get or
         * partNumber discovery has already taken part 1 by the time spreading is set up. */
        uint32_t spread_first_part;

        /* How many parts the spread has handed out so far. The only one of these fields that changes
         * after setup, and the index the rotation is derived from: the region is this modulo
         * `spread_num_regions`, and the position within that region is this divided by the same. In the
         * example above 0 yields part 2, 1 yields part 6, 2 yields part 9, and 3 wraps back to
         * region 0 for part 3. */
        uint32_t spread_parts_handed_out;

        uint32_t object_range_known : 1;

        /* True if object_range_known, and it's found to be empty.
         * If this is true, ignore object_range_start and object_range_end */
        uint32_t object_range_empty : 1;
        uint32_t head_object_sent : 1;
        uint32_t head_object_completed : 1;
        uint32_t read_window_warning_issued : 1;
    } synced_data;

    uint32_t initial_message_has_range_header : 1;
    uint32_t initial_message_has_if_match_header : 1;
};

AWS_EXTERN_C_BEGIN

/* Creates a new auto-ranged get meta request.  This will do multiple parallel ranged-gets when appropriate. */
AWS_S3_API struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_get_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    size_t part_size,
    bool part_size_set,
    const struct aws_s3_meta_request_options *options);

AWS_EXTERN_C_END

#endif /* AWS_S3_AUTO_RANGED_GET_H */
