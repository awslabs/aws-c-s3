# A simple HTTP server implemented using h11 and Trio:
#   http://trio.readthedocs.io/en/latest/index.html
#

import datetime
import email.utils
import json
from itertools import count
from urllib.parse import parse_qs, urlparse
import os
from enum import Enum

import trio

import h11

MAX_RECV = 2**16
TIMEOUT = 10


class S3Opts(Enum):
    CreateMultipartUpload = 1
    CompleteMultipartUpload = 2
    UploadPart = 3
    AbortMultipartUpload = 4


base_dir = os.path.dirname(os.path.realpath(__file__))

# We are using email.utils.format_datetime to generate the Date header.
# It may sound weird, but it actually follows the RFC.
# Please see: https://stackoverflow.com/a/59416334/14723771
#
# See also:
# [1] https://www.rfc-editor.org/rfc/rfc9110#section-5.6.7
# [2] https://www.rfc-editor.org/rfc/rfc7231#section-7.1.1.1
# [3] https://www.rfc-editor.org/rfc/rfc5322#section-3.3


def format_date_time(dt=None):
    """Generate a RFC 7231 / RFC 9110 IMF-fixdate string"""
    if dt is None:
        dt = datetime.datetime.now(datetime.timezone.utc)
    return email.utils.format_datetime(dt, usegmt=True)


################################################################
# I/O adapter: h11 <-> trio
################################################################

# The core of this could be factored out to be usable for trio-based clients
# too, as well as servers. But as a simplified pedagogical example we don't
# attempt this here.
class TrioHTTPWrapper:
    _next_id = count()

    def __init__(self, stream):
        self.stream = stream
        self.conn = h11.Connection(h11.SERVER)
        # Our Server: header
        self.ident = " ".join(
            ["h11-example-trio-server/{}".format(h11.__version__),
             h11.PRODUCT_ID]
        ).encode("ascii")
        # A unique id for this connection, to include in debugging output
        # (useful for understanding what's going on if there are multiple
        # simultaneous clients).
        self._obj_id = next(TrioHTTPWrapper._next_id)

    async def send(self, event):
        # The code below doesn't send ConnectionClosed, so we don't bother
        # handling it here either -- it would require that we do something
        # appropriate when 'data' is None.
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
        # When this method is called, it's because we definitely want to kill
        # this connection, either as a clean shutdown or because of some kind
        # of error or loss-of-sync bug, and we no longer care if that violates
        # the protocol or not. So we ignore the state of self.conn, and just
        # go ahead and do the shutdown on the socket directly. (If you're
        # implementing a client you might prefer to send ConnectionClosed()
        # and let it raise an exception if that violates the protocol.)
        #
        try:
            await self.stream.send_eof()
        except trio.BrokenResourceError:
            # They're already gone, nothing to do
            return
        # Wait and read for a bit to give them a chance to see that we closed
        # things, but eventually give up and just close the socket.
        # XX FIXME: possibly we should set SO_LINGER to 0 here, so
        # that in the case where the client has ignored our shutdown and
        # declined to initiate the close themselves, we do a violent shutdown
        # (RST) and avoid the TIME_WAIT?
        # it looks like nginx never does this for keepalive timeouts, and only
        # does it for regular timeouts (slow clients I guess?) if explicitly
        # enabled ("Default: reset_timedout_connection off")
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
            ("Date", format_date_time().encode("ascii")),
            ("Server", self.ident),
        ]

    def info(self, *args):
        # Little debugging method
        print("{}:".format(self._obj_id), *args)


################################################################
# Server main loop
################################################################

# General theory:
#
# If everything goes well:
# - we'll get a Request
# - our response handler will read the request body and send a full response
# - that will either leave us in MUST_CLOSE (if the client doesn't
#   support keepalive) or DONE/DONE (if the client does).
#
# But then there are many, many different ways that things can go wrong
# here. For example:
# - we don't actually get a Request, but rather a ConnectionClosed
# - exception is raised from somewhere (naughty client, broken
#   response handler, whatever)
#   - depending on what went wrong and where, we might or might not be
#     able to send an error response, and the connection might or
#     might not be salvagable after that
# - response handler doesn't fully read the request or doesn't send a
#   full response
#
# But these all have one thing in common: they involve us leaving the
# nice easy path up above. So we can just proceed on the assumption
# that the nice easy thing is what's happening, and whenever something
# goes wrong do our best to get back onto that path, and h11 will keep
# track of how successful we were and raise new errors if things don't work
# out.
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


async def send_response_from_json(wrapper, response_json_path, chunked=False):
    with open(response_json_path, 'r') as f:
        data = json.load(f)

        status_code = data['status']
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
            await wrapper.send(h11.Data(data=body.encode()))

        await wrapper.send(h11.EndOfMessage())


async def send_mock_s3_response(wrapper, request_type, path):
    response_file = os.path.join(
        base_dir, request_type.name, f"{path[1:]}.json")
    if os.path.exists(response_file) == False:
        wrapper.info(response_file, "not exist, using the default response")
        response_file = os.path.join(
            base_dir, request_type.name, f"default.json")
    await send_response_from_json(wrapper, response_file)


async def maybe_send_error_response(wrapper, exc):
    # If we can't send an error, oh well, nothing to be done
    wrapper.info("trying to send error response...")
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


async def handle_mock_s3_request(wrapper, request):
    parsed_path, parsed_query = parse_request_path(
        request.target.decode("ascii"))
    if request.method == b"POST":
        if parsed_path.query == "uploads":
            # POST /{Key+}?uploads HTTP/1.1 -- Create MPU
            request_type = S3Opts.CreateMultipartUpload
        else:
            # POST /Key+?uploadId=UploadId HTTP/1.1 -- Complete MPU
            request_type = S3Opts.CompleteMultipartUpload

    elif request.method == b"PUT":
        request_type = S3Opts.UploadPart
    elif request.method == b"DELETE":
        request_type = S3Opts.AbortMultipartUpload
    else:
        # TODO: support more type.
        request_type = S3Opts.CreateMultipartUpload

    while True:
        event = await wrapper.next_event()
        if type(event) is h11.EndOfMessage:
            break
        assert type(event) is h11.Data

    await send_mock_s3_response(
        wrapper, request_type, parsed_path.path
    )


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
