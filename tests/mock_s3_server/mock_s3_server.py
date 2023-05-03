# A simple HTTP server implemented using h11 and Trio:
#   http://trio.readthedocs.io/en/latest/index.html
#
#   S3 Mock server logic starts from handle_mock_s3_request

import json
from itertools import count
from urllib.parse import parse_qs, urlparse
import os
from enum import Enum

import trio

import h11

MAX_RECV = 2**16
TIMEOUT = 120  # this must be higher than any response's "delay" setting

VERBOSE = False
SHOULD_THROTTLE = True


class S3Opts(Enum):
    CreateMultipartUpload = 1
    CompleteMultipartUpload = 2
    UploadPart = 3
    AbortMultipartUpload = 4
    GetObject = 5
    ListParts = 6


base_dir = os.path.dirname(os.path.realpath(__file__))


class TrioHTTPWrapper:
    _next_id = count()

    def __init__(self, stream):
        self.stream = stream
        self.should_throttle = SHOULD_THROTTLE
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
            go_ahead = h11.InformationalResponse(
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

def parse_request_path(request_path):
    parsed_path = urlparse(request_path)
    parsed_query = parse_qs(parsed_path.query)
    return parsed_path, parsed_query


async def send_simple_response(wrapper, status_code, content_type, body):
    wrapper.info("Sending", status_code, "response with", len(body), "bytes")
    headers = wrapper.basic_headers()
    headers.append(("Content-Type", content_type))
    headers.append(("Content-Length", str(len(body))))
    res = h11.Response(status_code=status_code, headers=headers)
    await wrapper.send(res)
    await wrapper.send(h11.Data(data=body))
    await wrapper.send(h11.EndOfMessage())


async def send_response_from_json(wrapper, response_json_path, chunked=False, generate_body=False, generate_body_size=0, head_request=False):
    wrapper.info("sending response from json file: ", response_json_path,
                 ".\n generate_body: ", generate_body, "generate_body_size: ", generate_body_size)
    with open(response_json_path, 'r') as f:
        data = json.load(f)

    # if response has delay, then sleep before sending it
    delay = data.get('delay', 0)
    if delay > 0:
        assert delay < TIMEOUT
        await trio.sleep(delay)

    status_code = data['status']
    if generate_body:
        # generate body with a specific size instead
        body = "a" * generate_body_size
    else:
        body = "\n".join(data['body'])
    wrapper.info("Sending", status_code,
                 "response with", len(body), "bytes")

    headers = wrapper.basic_headers()
    for header in data['headers'].items():
        headers.append((header[0], header[1]))

    if chunked:
        headers.append(('Transfer-Encoding', "chunked"))
        res = h11.Response(status_code=status_code, headers=headers)
        await wrapper.send(res)
        await wrapper.send(h11.Data(data=b"%X\r\n%s\r\n" % (len(body), body.encode())))
    else:
        headers.append(("Content-Length", str(len(body))))
        res = h11.Response(status_code=status_code, headers=headers)
        await wrapper.send(res)
        if head_request:
            await wrapper.send(h11.EndOfMessage())
            return
        await wrapper.send(h11.Data(data=body.encode()))

    await wrapper.send(h11.EndOfMessage())


async def send_mock_s3_response(wrapper, request_type, path, generate_body=False, generate_body_size=0, head_request=False):
    response_file = os.path.join(
        base_dir, request_type.name, f"{path[1:]}.json")
    if os.path.exists(response_file) == False:
        wrapper.info(response_file, "not exist, using the default response")
        response_file = os.path.join(
            base_dir, request_type.name, f"default.json")
    if "throttle" in response_file:
        # We throttle the request half the time to make sure it succeeds after a retry
        if wrapper.should_throttle is False:
            wrapper.info("Skipping throttling")
            response_file = os.path.join(
                base_dir, request_type.name, f"default.json")
        else:
            wrapper.info("Throttling")
        # Flip the flag
        wrapper.should_throttle = not wrapper.should_throttle
    await send_response_from_json(wrapper, response_file, generate_body=generate_body, generate_body_size=generate_body_size)


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


def get_request_header_value(request, header_name):
    for header in request.headers:
        if header[0].decode("utf-8").lower() == header_name.lower():
            return header[1].decode("utf-8")
    return None


def handle_get_object_modified(start_range, end_range, request):
    data_length = end_range - start_range

    if start_range == 0:
        return "/get_object_modified_first_part", data_length, True
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
                return "/get_object_modified_success", data_length, False
            return "/get_object_modified_failure", data_length, False


def handle_get_object(request, parsed_path):

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
    elif parsed_path.path == "/get_object_invalid_response_missing_content_range":
        return "/get_object_invalid_response_missing_content_range", data_length, False
    elif parsed_path.path == "/get_object_invalid_response_missing_etags":
        return "/get_object_invalid_response_missing_etags", data_length, False

    return parsed_path.path, data_length, True


def handle_list_parts(parsed_path):
    if parsed_path.path == "/multiple_list_parts":
        if parsed_path.query.find("part-number-marker") != -1:
            return "/multiple_list_parts_2"
        else:
            return "/multiple_list_parts_1"
    return parsed_path.path


async def handle_mock_s3_request(wrapper, request):
    parsed_path, parsed_query = parse_request_path(
        request.target.decode("ascii"))
    response_path = parsed_path.path
    generate_body = False
    generate_body_size = 0
    method = request.method.decode("utf-8")

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
            response_path = handle_list_parts(parsed_path)
        else:
            request_type = S3Opts.GetObject
            response_path, generate_body_size, generate_body = handle_get_object(
                request, parsed_path)
    else:
        # TODO: support more type.
        wrapper.info("unsupported request:", request)
        request_type = S3Opts.CreateMultipartUpload

    while True:
        event = await wrapper.next_event()
        if type(event) is h11.EndOfMessage:
            break
        assert type(event) is h11.Data

    await send_mock_s3_response(
        wrapper, request_type, response_path, generate_body=generate_body, generate_body_size=generate_body_size, head_request=method == "HEAD")


async def serve(port):
    print("listening on http://localhost:{}".format(port))
    try:
        await trio.serve_tcp(http_serve, port)
    except KeyboardInterrupt:
        print("KeyboardInterrupt - shutting down")


################################################################
# Run the server
################################################################
if __name__ == "__main__":
    trio.run(serve, 8080)
