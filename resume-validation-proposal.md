# Download Resume Validation Proposal

## Background

The CRT download resume token restores the completed-parts bitmap so a resumed
meta request skips already-downloaded parts and fetches the rest. The question
is where and how to validate that the S3 object and the local file are still
the same as they were at pause time.

The AWS SDK for Java v2 answers this with a HeadObject pre-flight. This
proposal answers it with validation piggybacked on the resumed GET requests
themselves: no extra round trip, and a specific error code that lets SDKs
implement their own restart policy.

## Java SDK v2 Resume Path (for reference)

Entry point: `S3TransferManager.resumeDownloadFile(ResumableFileDownload)` in
[GenericS3TransferManager.java](https://github.com/aws/aws-sdk-java-v2/blob/master/services-custom/s3-transfer-manager/src/main/java/software/amazon/awssdk/transfer/s3/internal/GenericS3TransferManager.java).

Token fields
([ResumableFileDownload](https://docs.aws.amazon.com/java/api/latest/software/amazon/awssdk/transfer/s3/model/ResumableFileDownload.html)):

- `bytesTransferred` (long) -- bytes transferred so far
- `completedParts` (List\<Integer\>) -- part numbers completed and saved to
  file; non-empty only for multipart downloads
- `fileLastModified` (Instant, required) -- local file mtime at pause
- `s3ObjectLastModified` (Optional\<Instant\>) -- S3 Last-Modified at pause
- `s3ObjectEtag` (Optional\<String\>) -- S3 ETag at pause
- `totalSizeInBytes` (OptionalLong) -- expected content length
- `downloadFileRequest` -- the original request to re-issue

Flow:

1. If the multipart download context is already complete, return the completed
   download without any request.
2. Issue a **HeadObject** pre-flight (bucket + key).
3. In
   [ResumableRequestConverter.toDownloadFileRequestAndTransformer](https://github.com/aws/aws-sdk-java-v2/blob/master/services-custom/s3-transfer-manager/src/main/java/software/amazon/awssdk/transfer/s3/internal/utils/ResumableRequestConverter.java),
   run three modification checks:
   - `etagModified` -- token's `s3ObjectEtag` (when present) vs HeadObject's
     ETag, client-side string compare.
   - `s3ObjectModified` -- token's `s3ObjectLastModified` vs HeadObject's
     `Last-Modified`, exact equality.
   - `fileModified` -- local file check in
     [FileUtils.fileNotModified](https://github.com/aws/aws-sdk-java-v2/blob/master/services-custom/s3-transfer-manager/src/main/java/software/amazon/awssdk/transfer/s3/internal/utils/FileUtils.java):
     mtime equality **and** file length == `bytesTransferred`. The length
     check compensates for platforms where `File.lastModified()` lacks
     millisecond precision
     ([JDK-8177809](https://bugs.java.com/bugdatabase/view_bug.do?bug_id=8177809)).
4. Any check true -> **silent restart from the beginning** (CREATE_OR_REPLACE
   transformer, debug log only).
5. Otherwise resume, in one of two modes:
   - **Part-GET mode** (`completedParts` non-empty): reuse the original
     request with the `MultipartDownloadResumeContext` execution attribute;
     the multipart machinery skips completed parts. Transformer is
     `WRITE_TO_POSITION` at position 0 with `failureBehavior(LEAVE)`. Supports
     gaps between completed parts.
   - **Ranged-GET mode**: `Range: bytes=<bytesTransferred>-<contentLength>`
     with a `CREATE_OR_APPEND` transformer. Linear high-water mark, no gaps.
6. Both resumed modes pin `ifUnmodifiedSince(headObjectResponse.lastModified())`
   on the GetObject.

Cost: one extra HeadObject round trip on every resume. Behavior on change:
silent restart, invisible to the caller.

## CRT Proposal

### Design

1. **Local file check before starting the resume** (file delivery only).
   Compare the local file's current mtime against the token's
   `file_last_modified_epoch_ns` before the meta request schedules any work.

2. **No HeadObject pre-flight.** The resumed meta request issues parallel
   ranged GETs for the missing parts, exactly like a normal download.

3. **Validate on the first successful GetObject response.** The first part
   response that arrives carries everything a HeadObject would have provided.
   Validate it against the resume token:
   - `Last-Modified` response header vs token's `s3_object_last_modified`,
     **only when the token carries it**. The field is optional; when absent
     the check is skipped. ETag covers the integrity guarantee -- the
     Last-Modified check only adds detection of a same-etag re-upload, which
     delivers identical bytes.
   - Object size vs token's `object_size`, from the `Content-Range` total
     (or `Content-Length` where applicable).
   - ETag is already enforced server-side: every resumed part request carries
     `If-Match` with the token's etag (a required token field), so a changed
     object fails that part with `412 Precondition Failed` atomically with
     the fetch.

   Precondition semantics per
   [RFC 9110 Section 13.2.1](https://www.rfc-editor.org/rfc/rfc9110.html#section-13.2.1):
   preconditions are evaluated once per request, "just before it would
   process the request content (if any) or perform the action associated
   with the request method". Each parallel part request is evaluated
   independently; there is no re-evaluation while a response streams. Note
   also that when `If-Match` is present, `If-Unmodified-Since` is ignored
   ([Section 13.1.4](https://www.rfc-editor.org/rfc/rfc9110.html#section-13.1.4):
   "A recipient MUST ignore If-Unmodified-Since if the request contains an
   If-Match header field"; precedence order in
   [Section 13.2.2](https://www.rfc-editor.org/rfc/rfc9110.html#section-13.2.2)).

4. **Fail loudly with a specific error.** Any validation failure fails the
   meta request with a dedicated resume error code (a new
   `AWS_ERROR_S3_RESUME_VALIDATION_FAILED`, name to be finalized). The error
   is delivered through the normal finish callback.

5. **412 handling depends on whether validation has run.** While the token
   validation has not yet happened (no part response has been validated), a
   `412 Precondition Failed` from `If-Match` is reported as the resume
   validation error -- at that point the 412 means the resume premise itself
   is broken. Once validation has completed successfully, a later 412 follows
   the normal error path.

### Division of responsibility

- CRT detects the mismatch and reports it with a specific, retryable-by-design
  error code. It does not silently restart.
- SDKs that want Java-style silent restart catch the resume error code and
  re-issue the download without a token. The restart policy stays where the
  retry policy already lives.

### Why this shape

- **No extra round trip.** Validation rides on part requests the resume must
  send anyway. Java pays one HeadObject per resume; this pays nothing.
- **ETag validation is stronger than the HEAD approach.** `If-Match` is
  evaluated by S3 atomically with each part fetch. A HEAD-then-GET sequence
  has a window where the object changes between the two calls; per-request
  `If-Match` does not.
- **The error code preserves SDK freedom.** Silent restart is a policy
  decision. Different SDKs (and different customers) may prefer surfacing the
  failure. CRT reporting a precise error lets each SDK choose, instead of
  baking one policy into the C layer.
- **Local file check stays cheap and early.** It needs no network and gates
  whether resume state is usable at all, so it runs once before any request.

### Failure matrix

- Local file missing or mtime mismatch at start -> resume validation error
  before any request is sent.
- Object replaced (different etag), detected before validation has run ->
  first part request fails with 412 via `If-Match`; reported as the resume
  validation error.
- Object replaced after validation has run -> 412 follows the normal error
  path.
- Object rewritten with identical etag but different Last-Modified (same
  content re-uploaded) -> caught by the Last-Modified check on the first
  successful response.
- Object size differs from token -> caught by the Content-Range/Content-Length
  check on the first successful response.

### Decisions

- **Validation runs once**, on the first successful part response. Subsequent
  responses are covered by `If-Match` on every part request: an object change
  mid-download fails 412 server-side. The only case per-response validation
  would add is a same-etag re-upload mid-download, which delivers identical
  bytes and is harmless. Per-response checks would be computationally free but
  add no protection.
- **One error code** covers both the local file check and the remote object
  checks. The error message distinguishes the cause for logging and debugging.
- **412 remapping is conditional**: 412 before validation completes -> resume
  validation error; 412 after -> normal error path.
- **`s3_object_last_modified` stays optional in the token.** When absent, the
  Last-Modified check is skipped. ETag via `If-Match` (required field) is the
  integrity guarantee; the Last-Modified check is supplementary.
