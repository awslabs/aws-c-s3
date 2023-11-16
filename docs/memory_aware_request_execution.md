CRT S3 client was designed with throughput as a primary goal. As such, the client
scales resource usage, such as number of parallel requests in flight, to achieve
target throughput. The client creates buffers to hold data it is sending or
receiving for each request and scaling requests in flight has direct impact on
memory used. In practice, setting high target throughput or larger part size can
lead to high observed memory usage. 

To mitigate high memory usages, memory reuse improvements were recently added to
the client along with options to limit max memory used. The following sections
will go into more detail on aspects of those changes and how the affect the
client. 

### Memory Reuse
At the basic level, CRT S3 client starts with a meta request for operation like
put or get, breaks it into smaller part-sized requests and executes those in
parallel. CRT S3 client used to allocate part sized buffer for each of those
requests and release it right after the request was done. That approach,
resulted in a lot of very short lived allocations and allocator thrashing,
overall leading to memory use spikes considerably higher than whats needed. To
address that, the client is switching to a pooled buffer approach, discussed
below. 

Note: approach described below is work in progress and concentrates on improving
the common cases (default 8mb part sizes and part sizes smaller than 64mb).

Several observations about the client usage of buffers:
- Client does not automatically switch to buffers above default 8mb for upload, until
  upload passes 10,000 parts (~80 GB).
- Get operations always use either the configured part size or default of 8mb.
  Part size for get is not adjusted, since there is no 10,000 part limitation.
- Both Put and Get operations go through fill and drain phases. Ex. for Put, the
  client first schedules a number of reads to 'fil' the buffers from the source
  and as those reads complete, the buffer are send over to the networking layer
  are 'drained'
- individual uploadParts or ranged gets operations typically have a similar
  lifespan (with some caveats). in practice part buffers are acquired/released
  in bulk at the same time

The buffer pooling takes advantage of some of those allocation patterns and
works as follows. 
The memory is split into primary and secondary areas. Secondary area is used for
requests with part size bigger than a predefined value (currently 128mb)
allocations from it got directly to allocator and are effectively old way of
doing things. 

Primary memory area is split into blocks of fixed size (currently 128mb). Blocks
are allocated on demand. Buffers are 'acquired' from those block, either by
finding an empty spot in existing block or creating a new block. Allocations
from blocks are simplified to not have to worry about gaps in block, by
allocating only from the back of the block - i.e. each allocation moves the
available space pointer in the block forward and increments the number of
allocations in that block. Once available space pointer reaches end of block, no
more allocations are done from that block until all the previous allocations are
released (i.e. block is put back into use, when allocation count reaches 0).

Blocks are kept around while there are ongoing requests and are released async,
when there is low pressure on memory.

### Scheduling
Running out of memory is a terminal condition within CRT and in general its not
practical to try to set overall memory limit on all allocations, since it
dramatically increases the complexity of the code that deals with cases where
only part of a memory was allocated for a task.

Comparatively, majority of memory usage within S3 Client comes from buffers
allocated for Put/Get parts. So to control memory usage, the client will
concentrate on controlling the number of buffers allocated. Effectively, this
boils down to a back pressure mechanism of limiting the number of parts
scheduled as memory gets closer to the limit. Memory used for other resources,
ex. http connections data, various supporting structures, are not actively
controlled and instead some memory is taken out from overall limit.

Overall, scheduling does a best-effort memory limiting. At the time of
scheduling, the client reserves memory by using buffer pool ticketing mechanism.
Buffer is acquired from the pool using the ticket as close to the usage as
possible (this approach peaks at lower mem usage than preallocating all mem
upfront because buffers cannot be used right away, ex reading from file will
fill buffers slower than they are sent, leading to decent amount of buffer reuse)
Reservation mechanism is approximate and in some cases can lead to actual memory
usage being higher once tickets are redeemed. The client reserves some memory to
mitigate overflows like that.
