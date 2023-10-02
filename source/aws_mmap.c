#include "aws/s3/private/aws_mmap.h"
#include <aws/common/allocator.h>

#ifdef _WIN32
#    include <windows.h>
#else
#    include <sys/mman.h>
#    include <sys/stat.h>
#    include <unistd.h>
#endif /* _WIN32 */

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>

#ifdef _WIN32
struct aws_mmap_context *aws_mmap_new(const char *file_name) {
    return "";
}
#else

struct aws_mmap_context_posix_impl {
    struct aws_allocator *allocator;
    int fd;
};

struct aws_mmap_context *s_mmap_context_destroy(struct aws_mmap_context *context) {
    if (!context) {
        return NULL;
    }
    struct aws_mmap_context_posix_impl *impl = context->impl;
    if (impl->fd) {
        close(impl->fd);
    }
    aws_mem_release(impl->allocator, context);
    return NULL;
}

struct aws_mmap_context *aws_mmap_context_release(struct aws_mmap_context *context) {
    return s_mmap_context_destroy(context);
}

struct aws_mmap_context *aws_mmap_context_new(struct aws_allocator *allocator, const char *file_name) {
    struct aws_mmap_context *context = NULL;
    struct aws_mmap_context_posix_impl *impl = NULL;
    aws_mem_acquire_many(
        allocator, 2, &context, sizeof(struct aws_mmap_context), &impl, sizeof(struct aws_mmap_context_posix_impl));

    impl->fd = open(file_name, O_RDWR);
    impl->allocator = allocator;
    if (impl->fd == -1) {
        goto error;
    }

    struct stat file_stat;
    if (fstat(impl->fd, &file_stat) == -1) {
        goto error;
    }
    void *mapped_data = mmap(NULL, file_stat.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, impl->fd, 0);
    if (mapped_data == MAP_FAILED) {
        goto error;
    }

    context->content = (char *)mapped_data;
    context->impl = impl;
    return context;
error:
    /* TODO: LOG and raise AWS ERRORs */
    return s_mmap_context_destroy(context);
}
#endif /* _WIN32 */
