# A simple HTTP server implemented using h11 and Trio:
#   http://trio.readthedocs.io/en/latest/index.html
#
#   S3 Mock server logic starts from handle_mock_s3_request

from dataclasses import dataclass
import json
from itertools import count
from urllib.parse import parse_qs, urlparse
import os
from typing import Optional
from enum import Enum

import trio

import h11

MAX_RECV = 2**16
TIMEOUT = 120  # this must be higher than any response's "delay" setting

VERBOSE = False

# Flags to keep between requests
SHOULD_THROTTLE = True
RETRY_REQUEST_COUNT = 0


base_dir = os.path.dirname(os.path.realpath(__file__))


class S3Opts(Enum):
    CreateMultipartUpload = 1
    CompleteMultipartUpload = 2
    UploadPart = 3
    AbortMultipartUpload = 4
    GetObject = 5
    ListParts = 6


@dataclass
class Response:
    status_code: int
    delay: int
    headers: any
    data: str
    chunked: bool
    head_request: bool


@dataclass
class ResponseConfig:
    path: str
    disconnect_after_headers = False
    generate_body_size: Optional[int] = None
    json_path: str = None
    throttle: bool = False
    force_retry: bool = False

    def _resolve_file_path(self, wrapper, request_type):
        global SHOULD_THROTTLE
        if self.json_path is None:
            response_file = os.path.join(
                base_dir, request_type.name, f"{self.path[1:]}.json")
            if os.path.exists(response_file) == False:
                wrapper.info(
                    response_file, "not exist, using the default response")
                response_file = os.path.join(
                    base_dir, request_type.name, f"default.json")
            if "throttle" in response_file:
                # We throttle the request half the time to make sure it succeeds after a retry
                if SHOULD_THROTTLE is False:
                    wrapper.info("Skipping throttling")
                    response_file = os.path.join(
                        base_dir, request_type.name, f"default.json")
                else:
                    wrapper.info("Throttling")
                # Flip the flag
                SHOULD_THROTTLE = not SHOULD_THROTTLE
            self.json_path = response_file

    def resolve_response(self, wrapper, request_type, chunked=False, head_request=False):
        self._resolve_file_path(wrapper, request_type)
        wrapper.info("resolving response from json file: ", self.json_path,
                     ".\n generate_body_size: ", self.generate_body_size)
        with open(self.json_path, 'r') as f:
            data = json.load(f)

        # if response has delay, then sleep before sending it
        delay = data.get('delay', 0)
        status_code = data['status']
        if self.generate_body_size is not None:
            # generate body with a specific size instead
            body = "a" * self.generate_body_size
        else:
            body = "\n".join(data['body'])

        headers = wrapper.basic_headers()
        content_length_set = False
        for header in data['headers'].items():
            headers.append((header[0], str(header[1])))
            if header[0].lower() == "content-length":
                content_length_set = True

        if chunked:
            headers.append(('Transfer-Encoding', "chunked"))
        else:
            if self.force_retry:
                # Use a long `content-length` header to trigger error when we try to send EOM.
                # so that the server will close connection after we send the header.
                headers.append(("Content-Length", str(123456)))
            elif content_length_set is False:
                headers.append(("Content-Length", str(len(body))))

        response = Response(status_code=status_code, delay=delay, headers=headers,
                            data=body, chunked=chunked, head_request=head_request)

        return response


class TrioHTTPWrapper:
    _next_id = count()

    def __init__(self, stream):
        self.stream = stream
        self.conn = h11.Connection(h11.SERVER)
        # A unique id for this connection, to include in debugging output
        # (useful for understanding what's going on if there are multiple
        # simultaneous clients).
        self._obj_id = next(TrioHTTPWrapper._next_id)

    async def send(self, event):
        assert type(event) is not h11.ConnectionClosed
        data = self.conn.send(event)
        try:
            await self.stream.send_all(data)
        except BaseException:
            # If send_all raises an exception (especially trio.Cancelled),
            # we have no choice but to give it up.
            self.conn.send_failed()
            raise

    async def _read_from_peer(self):
        if self.conn.they_are_waiting_for_100_continue:
            self.info("Sending 100 Continue")
            go_ahead = h11.InformationalResponseConfig(
                status_code=100, headers=self.basic_headers()
            )
            await self.send(go_ahead)
        try:
            data = await self.stream.receive_some(MAX_RECV)
        except ConnectionError:
            # They've stopped listening. Not much we can do about it here.
            data = b""
        self.conn.receive_data(data)

    async def next_event(self):
        while True:
            event = self.conn.next_event()
            if event is h11.NEED_DATA:
                await self._read_from_peer()
                continue
            return event

    async def shutdown_and_clean_up(self):
        try:
            await self.stream.send_eof()
        except trio.BrokenResourceError:
            return

        with trio.move_on_after(TIMEOUT):
            try:
                while True:
                    # Attempt to read until EOF
                    got = await self.stream.receive_some(MAX_RECV)
                    if not got:
                        break
            except trio.BrokenResourceError:
                pass
            finally:
                await self.stream.aclose()

    def basic_headers(self):
        # HTTP requires these headers in all responses (client would do
        # something different here)
        return [
            ("Server", "mock_s3_server"),
        ]

    def info(self, *args):
        # Little debugging method
        if VERBOSE:
            print("{}:".format(self._obj_id), *args)


################################################################
# Server main loop
################################################################


async def send_simple_response(wrapper, status_code, content_type, body):
    wrapper.info("Sending", status_code, "response with", len(body), "bytes")
    headers = wrapper.basic_headers()
    headers.append(("Content-Type", content_type))
    headers.append(("Content-Length", str(len(body))))
    res = h11.Response(status_code=status_code, headers=headers)
    await wrapper.send(res)
    await wrapper.send(h11.Data(data=body))
    await wrapper.send(h11.EndOfMessage())


async def maybe_send_error_response(wrapper, exc):
    if wrapper.conn.our_state not in {h11.IDLE, h11.SEND_RESPONSE}:
        wrapper.info("...but I can't, because our state is",
                     wrapper.conn.our_state)
        return
    try:
        if isinstance(exc, h11.RemoteProtocolError):
            status_code = exc.error_status_hint
        elif isinstance(exc, trio.TooSlowError):
            status_code = 408  # Request Timeout
        else:
            status_code = 500
        body = str(exc).encode("utf-8")
        await send_simple_response(
            wrapper, status_code, "text/plain; charset=utf-8", body
        )
    except Exception as exc:
        wrapper.info("error while sending error response:", exc)


async def http_serve(stream):
    wrapper = TrioHTTPWrapper(stream)
    wrapper.info("Got new connection")
    while True:
        assert wrapper.conn.states == {
            h11.CLIENT: h11.IDLE, h11.SERVER: h11.IDLE}

        try:
            with trio.fail_after(TIMEOUT):
                wrapper.info("Server main loop waiting for request")
                event = await wrapper.next_event()
                wrapper.info("Server main loop got event:", event)
                if type(event) is h11.Request:
                    await handle_mock_s3_request(wrapper, event)
        except Exception as exc:
            wrapper.info("Error during response handler: {!r}".format(exc))
            await maybe_send_error_response(wrapper, exc)

        if wrapper.conn.our_state is h11.MUST_CLOSE:
            wrapper.info("connection is not reusable, so shutting down")
            await wrapper.shutdown_and_clean_up()
            return
        else:
            try:
                wrapper.info("trying to re-use connection")
                wrapper.conn.start_next_cycle()
            except h11.ProtocolError:
                states = wrapper.conn.states
                wrapper.info("unexpected state", states, "-- bailing out")
                await maybe_send_error_response(
                    wrapper, RuntimeError("unexpected state {}".format(states))
                )
                await wrapper.shutdown_and_clean_up()
                return


################################################################
# Actual response handlers
################################################################

# Helper function
async def send_response(wrapper, response):
    if response.delay > 0:
        assert response.delay < TIMEOUT
        await trio.sleep(response.delay)

    wrapper.info("Sending", response.status_code,
                 "response with", len(response.data), "bytes")

    res = h11.Response(status_code=response.status_code,
                       headers=response.headers)

    try:
        await wrapper.send(res)
    except Exception as e:
        print(e)

    if not response.head_request:
        if response.chunked:
            await wrapper.send(h11.Data(data=b"%X\r\n%s\r\n" % (len(response.data), response.data.encode())))
        else:
            await wrapper.send(h11.Data(data=response.data.encode()))

    await wrapper.send(h11.EndOfMessage())


def get_request_header_value(request, header_name):
    for header in request.headers:
        if header[0].decode("utf-8").lower() == header_name.lower():
            return header[1].decode("utf-8")
    return None


def handle_get_object_modified(start_range, end_range, request):
    data_length = end_range - start_range

    if start_range == 0:
        return ResponseConfig("/get_object_modified_first_part", generate_body_size=data_length)
    else:
        # Check the request header to make sure "If-Match" is set
        etag = get_request_header_value(request, "if-match")
        print(etag)
        # fetch Etag from the first_part response file
        response_file = os.path.join(
            base_dir, S3Opts.GetObject.name, f"get_object_modified_first_part.json")
        with open(response_file, 'r') as f:
            data = json.load(f)
            if data['headers']['ETag'] == etag:
                return ResponseConfig("/get_object_modified_success")
            return ResponseConfig("/get_object_modified_failure")


def handle_get_object(wrapper, request, parsed_path, head_request=False):
    global RETRY_REQUEST_COUNT
    response_config = ResponseConfig(parsed_path.path)
    if parsed_path.path == "/get_object_checksum_retry" and not head_request:
        RETRY_REQUEST_COUNT = RETRY_REQUEST_COUNT + 1

        if RETRY_REQUEST_COUNT == 1:
            wrapper.info("Force retry on the request")
            response_config.force_retry = True
    else:
        RETRY_REQUEST_COUNT = 0

    body_range_value = get_request_header_value(request, "range")

    if body_range_value:
        body_range = body_range_value.split("=")[1]
        start_range = int(body_range.split("-")[0])
        end_range = int(body_range.split("-")[1])
    else:
        # default length is 65535
        start_range = 0
        end_range = 65535

    data_length = end_range - start_range

    if parsed_path.path == "/get_object_modified":
        return handle_get_object_modified(start_range, end_range, request)
    elif parsed_path.path == "/get_object_invalid_response_missing_content_range" or parsed_path.path == "/get_object_invalid_response_missing_etags":
        # Don't generate the body for those requests
        return response_config

    response_config.generate_body_size = data_length
    return response_config


def handle_list_parts(parsed_path):
    if parsed_path.path == "/multiple_list_parts":
        if parsed_path.query.find("part-number-marker") != -1:
            return ResponseConfig("/multiple_list_parts_2")
        else:
            return ResponseConfig("/multiple_list_parts_1")
    return ResponseConfig(parsed_path.path)


async def handle_mock_s3_request(wrapper, request):
    parsed_path = urlparse(request.target.decode("ascii"))
    method = request.method.decode("utf-8")
    response_config = None

    if method == "POST":
        if parsed_path.query == "uploads":
            # POST /{Key+}?uploads HTTP/1.1 -- Create MPU
            request_type = S3Opts.CreateMultipartUpload
        else:
            # POST /Key+?uploadId=UploadId HTTP/1.1 -- Complete MPU
            request_type = S3Opts.CompleteMultipartUpload
    elif method == "PUT":
        request_type = S3Opts.UploadPart
    elif method == "DELETE":
        request_type = S3Opts.AbortMultipartUpload
    elif method == "GET" or method == "HEAD":
        if parsed_path.query.find("uploadId") != -1:
            # GET /Key+?max-parts=MaxParts&part-number-marker=PartNumberMarker&uploadId=UploadId HTTP/1.1 -- List Parts
            request_type = S3Opts.ListParts
            response_config = handle_list_parts(parsed_path)
        else:
            request_type = S3Opts.GetObject
            response_config = handle_get_object(
                wrapper, request, parsed_path, head_request=method == "HEAD")
    else:
        # TODO: support more type.
        wrapper.info("unsupported request:", request)
        request_type = S3Opts.CreateMultipartUpload

    while True:
        event = await wrapper.next_event()
        if type(event) is h11.EndOfMessage:
            break
        assert type(event) is h11.Data

    if response_config is None:
        response_config = ResponseConfig(parsed_path.path)

    response = response_config.resolve_response(
        wrapper, request_type, head_request=method == "HEAD")

    await send_response(wrapper, response)


################################################################
# Run the server
################################################################

async def serve(port):
    print("listening on http://localhost:{}".format(port))
    try:
        await trio.serve_tcp(http_serve, port)
    except KeyboardInterrupt:
        print("KeyboardInterrupt - shutting down")

if __name__ == "__main__":
    trio.run(serve, 8080)
