#!/usr/bin/env python3
from argparse import ArgumentParser
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime
import json
import sys
from time import perf_counter

ARG_PARSER = ArgumentParser(description="Scrape logs from CRT S3Client")
ARG_PARSER.add_argument('--log-in', required=True,
                        help="Log file to read (captured at TRACE|DEBUG level)")
ARG_PARSER.add_argument('--json-out', required=True,
                        help="JSON file to write")


class LogPattern:
    def __init__(self, topic: str, pattern: str):
        self._topic = topic
        self._pattern = re.compile(pattern)

    def match(self, line: 'LogLine') -> re.Match | None:
        if line.topic != self._topic:
            return None
        return self._pattern.match(line.msg)


LOG_LINE = re.compile(
    r'\[(?P<level>[^\]]+)\] \[(?P<date>[^\]]+)\] \[(?P<thread>[^\]]+)\] \[(?P<topic>[^\]]+)\] - (?P<msg>.*)')

EVENT_LOOP_THREAD_START = LogPattern(
    'event-loop', r'id=(?P<id>[^:]+): main loop started.*')

S3_META_REQUEST_START = LogPattern(
    'S3MetaRequest', r'id=(?P<id>[^ ]+) Created new.*')
S3_META_REQUEST_END_OK = LogPattern(
    'S3MetaRequest', r'id=(?P<id>[^ ]+) Meta request finished with error code 0.*')
S3_META_REQUEST_END_ERR = LogPattern(
    'S3MetaRequest', r'id=(?P<id>[^ ]+) Meta request cannot recover from error (?P<err_num>\d+).*')

S3_REQUEST_START = LogPattern(
    'S3MetaRequest', r'id=(?P<meta>[^:]+): Returning request (?P<req>[^ ]+) for part (?P<part_num>\d+) of (?P<num_parts>\d+)')
# S3_REQUEST_END = I'm not finding a simple way to do this with debug logs

S3_REQUEST_ATTEMPT_START = LogPattern(
    'S3MetaRequest', r'id=(?P<meta>[^:]+): (Prepared|Created) request (?P<req>[^ ]+) for part (?P<part_num>\d+).*')
S3_REQUEST_ATTEMPT_END = LogPattern(
    'S3MetaRequest', r'id=(?P<meta>[^:]+): Request (?P<req>[^ ]+) finished with error code (?P<err_num>\d+) \([^:]*: (?P<err_name>\w+).*\) and response status (?P<http_status>[0-9]+)')

HTTP_CONNECTION_START = LogPattern(
    'http-connection', r'id=(?P<id>[^:]+): HTTP/1.1 client connection established.')
HTTP_CONNECTION_END = LogPattern(
    'http-connection', r'(?P<id>[^:]+): Client shutdown completed with error \d+ \((?P<err_name>[^)]+)\).')

HTTP_STREAM_START = LogPattern(
    'http-stream', r'id=(?P<id>[^:]+): Created client request on connection=(?P<conn>[^:]+): (?P<method>[^ ]*) (?P<path>[^ ]+).*')
HTTP_STREAM_END_OK = LogPattern(
    'http-stream', r'id=(?P<id>[^:]+): Client request complete, response status: (?P<http_status>[0-9]+).*')
HTTP_STREAM_END_ERR = LogPattern(
    'http-stream', r'id=(?P<id>[^:]+): Stream completed with error code (?P<err_num>[^ ]+) \((?P<err_name>\w+).*')

S3_CLIENT_STATS = LogPattern(
    'S3ClientStats',
    (r'id=(?P<id>[^ ]+) '
     r'Requests-in-flight\(approx/exact\):(?P<approx>\d+)/(?P<exact>\d+)  '
     r'Requests-preparing:(?P<preparing>\d+)  '
     r'Requests-queued:(?P<queued>\d+)  '
     r'Requests-network\(get/put/default/total\):(?P<net_get>\d+)/(?P<net_put>\d+)/(?P<net_default>\d+)/(?P<net_total>\d+)  '
     r'Requests-streaming-waiting:(?P<streaming_waiting>\d+)  '
     r'Requests-streaming-response:(?P<streaming_response>\d+)  '
     r'Endpoints\(in-table/allocated\):(?P<endpoints_in_table>\d+)/(?P<endpoints_allocated>\d+)'))

BOTO_LOG = re.compile(
    r'(?P<date>\d{4}-\d{2}-\d{2}) (?P<time>\d{2}:\d{2}:\d{2},\d+) - (?P<thread>.+) - (?P<topic>.*) - (?P<level>.*) - (?P<msg>.*)')


@dataclass
class LogLine:
    num: int
    level: str
    date_str: str
    thread: str
    topic: str
    msg: str

    def date(self) -> datetime:
        return datetime.strptime(self.date_str, "%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass
class EventLoopThread:
    thread_id: str
    event_loop_id: str
    # filled in later...
    visual_size: int = None
    connections: list['HttpConnection'] = field(default_factory=list)


@dataclass
class HttpConnection:
    id: str
    start_time: float
    # filled in later...
    end_time: float = None
    error: str = None
    streams: list['HttpStream'] = field(default_factory=list)
    visual_idx: int = None


@dataclass
class HttpStream:
    id: str
    method: str
    path: str
    start_time: float
    # filled in later...
    end_time: float = None
    error: str = None
    http_status: int = None


@dataclass
class S3MetaRequest:
    id: str
    start_time: float
    # filled in later...
    end_time: float = None
    error_num: int = None
    http_status: int = None
    num_parts: int = None
    s3_requests_by_part_num: dict[int, 'S3Request'] = field(
        default_factory=dict)


@dataclass
class S3Request:
    id: str
    part_num: int
    start_time: float
    # filled in later...
    end_time: float = None
    error: str = None
    visual_idx: int = None
    attempts: list['S3RequestAttempt'] = field(default_factory=list)


@dataclass
class S3RequestAttempt:
    start_time: float
    # filled in later...
    end_time: float = None
    error_num: int = None
    error: str = None
    http_status: int = None


@dataclass
class S3ClientStat:
    in_flight_total: int
    preparing: int
    queued: int
    net_get: int
    net_put: int
    net_default: int
    net_total: int
    streaming_waiting: int
    streaming_response: int
    time: float


@dataclass
class S3Run:
    meta_requests: list[S3MetaRequest] = field(default_factory=list)
    threads: dict[str, EventLoopThread] = field(default_factory=dict)
    stats: list[S3ClientStat] = field(default_factory=list)
    max_time: float = 0


def log(msg: str):
    print(msg, file=sys.stderr)


class PerfTimer:
    def __init__(self, name):
        self.name = name

    def __enter__(self):
        self.start = perf_counter()

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            end = perf_counter()
            log(f"{self.name}: {end - self.start:.3f} sec")


class _Scraper:
    def __init__(self, log_filename: str):
        # these are the top-level datastructures, nothing is ever removed from them
        self.s3_run = S3Run()

        # These datastructures are keyed on ID, which is usually a memory address.
        # Any entry may be replaced if that same ID (memory address) is used again later by something else.
        self._http_connections: dict[str, HttpConnection] = {}
        self._http_streams: dict[str, HttpStream] = {}
        self._s3_requests: dict[str, S3Request] = {}
        self._meta_requests: dict[str, S3MetaRequest] = {}

        self._last_line = None

        with open(log_filename) as log_file:
            # Batch up all lines that occur in the same second and process them together.
            # We gather all lines in the same second so we can add fake microsecond offsets
            # so they don't overlap so much. We process them in batches
            # (vs the simpler thing of gathering all lines, then processing all lines)
            # so that we only have a few lines in memory at a time. These log files can get BIG.
            lines_same_second: list[LogLine] = []

            for line_num, line_txt in enumerate(log_file, start=1):
                line_txt = line_txt.strip()
                m = LOG_LINE.match(line_txt)
                if not m:
                    continue

                line = LogLine(
                    num=line_num,
                    level=m.group('level'),
                    date_str=m.group('date'),
                    thread=m.group('thread'),
                    topic=m.group('topic'),
                    msg=m.group('msg'),
                )

                self._last_line = line

                if lines_same_second and lines_same_second[0].date_str != line.date_str:
                    self._process_lines_same_second(lines_same_second)
                    lines_same_second.clear()

                lines_same_second.append(line)

            # process remaining lines
            if lines_same_second:
                self._process_lines_same_second(lines_same_second)

            # run post-processing on data
            self._post_processing()

    def _process_lines_same_second(self, lines: list[LogLine]):
        # Force the timestamps to be distinct by faking the microseconds.
        # So like, if there are 3 lines at: "2024-05-15T01:02:03Z"
        # Change them into: "2024-05-15T01:02:03.000000Z" "2024-05-15T01:02:03Z.333333Z" "2024-05-15T01:02:03.666666Z"
        for idx, line in enumerate(lines):
            microseconds = int(1000000.0 * (idx / len(lines)))
            line.date_str = f"{line.date_str[:-1]}.{microseconds:06}Z"

            try:
                self._process_line(line)
            except Exception as e:
                print(f'FAILED processing line {line.num}: {line.msg}')
                raise e

    def _process_line(self, line: LogLine):
        if not hasattr(self, 'start_date'):
            self.start_date = line.date()

        # EventLoopThread
        if m := EVENT_LOOP_THREAD_START.match(line):
            id = m.group('id')
            el_thread = EventLoopThread(line.thread, id)
            self.s3_run.threads[line.thread] = el_thread

        # S3MetaRequest
        elif m := S3_META_REQUEST_START.match(line):
            id = m.group('id')
            meta = S3MetaRequest(id=id,
                                 start_time=self._line_time(line))
            self._meta_requests[id] = meta
            self.s3_run.meta_requests.append(meta)

        elif m := S3_META_REQUEST_END_OK.match(line):
            id = m.group('id')
            meta = self._meta_requests[id]
            meta.error_num = 0
            meta.end_time = self._line_time(line)

        elif m := S3_META_REQUEST_END_ERR.match(line):
            # this line can occur multiple times, if multiple S3Requests fail
            id = m.group('id')
            meta = self._meta_requests[id]
            if not meta.error_num:
                meta.error_num = int(m.group('err_num'))
            meta.end_time = self._line_time(line)

        # S3Request
        elif m := S3_REQUEST_START.match(line):
            self._get_or_create_s3_request(line, m)

        # S3RequestAttempt
        elif m := S3_REQUEST_ATTEMPT_START.match(line):
            req_id = m.group('req')
            req = self._get_or_create_s3_request(line, m)
            attempt = S3RequestAttempt(start_time=self._line_time(line))
            req.attempts.append(attempt)

        elif m := S3_REQUEST_ATTEMPT_END.match(line):
            req_id = m.group('req')
            req = self._s3_requests[req_id]
            attempt = req.attempts[-1]
            attempt.end_time = self._line_time(line)
            attempt.error_num = int(m.group('err_num'))
            attempt.error = m.group('err_name')

        # HttpConnection
        elif m := HTTP_CONNECTION_START.match(line):
            conn = HttpConnection(id=m.group('id'),
                                  start_time=self._line_time(line))
            self._http_connections[conn.id] = conn
            # add to EventLoopThread
            self.s3_run.threads[line.thread].connections.append(conn)

        elif m := HTTP_CONNECTION_END.match(line):
            id = m.group('id')
            conn = self._http_connections[id]
            conn.end_time = self._line_time(line)
            conn.error = m.group('err_name')

        # HttpStream
        elif m := HTTP_STREAM_START.match(line):
            stream = HttpStream(id=m.group('id'),
                                method=m.group('method'),
                                path=m.group('path'),
                                start_time=self._line_time(line))
            self._http_streams[stream.id] = stream
            # add to HttpConnection
            conn_id = m.group('conn')
            self._http_connections[conn_id].streams.append(stream)

        elif m := HTTP_STREAM_END_OK.match(line):
            id = m.group('id')
            stream = self._http_streams[id]
            stream.http_status = m.group('http_status')
            stream.end_time = self._line_time(line)

        elif m := HTTP_STREAM_END_ERR.match(line):
            id = m.group('id')
            stream = self._http_streams[id]
            stream.error = m.group('err_name')
            stream.end_time = self._line_time(line)

        elif m := S3_CLIENT_STATS.match(line):
            self.s3_run.stats.append(S3ClientStat(
                in_flight_total=int(m.group('exact')),
                preparing=int(m.group('preparing')),
                queued=int(m.group('queued')),
                net_get=int(m.group('net_get')),
                net_put=int(m.group('net_put')),
                net_default=int(m.group('net_default')),
                net_total=int(m.group('net_total')),
                streaming_waiting=int(m.group('streaming_waiting')),
                streaming_response=int(m.group('streaming_response')),
                time=self._line_time(line),
            ))

    def _get_or_create_s3_request(self, line: LogLine, m: re.Match):
        # Some requests don't log a creation line, so `m` may be S3_REQUEST_START or S3_REQUEST_ATTEMPT_START
        req_id = m.group('req')

        # if S3Request already exists, return it
        if s3_req := self._s3_requests.get(req_id):
            return s3_req

        # otherwise, create it
        s3_req = S3Request(id=req_id,
                           start_time=self._line_time(line),
                           part_num=int(m.group('part_num')))
        self._s3_requests[s3_req.id] = s3_req

        # add to S3MetaRequest
        meta_id = m.group('meta')
        meta = self._meta_requests[meta_id]
        meta.s3_requests_by_part_num[s3_req.part_num] = s3_req

        # S3_REQUEST_START has num_parts, but S3_REQUEST_ATTEMPT_START doesn't
        if 'num_parts' in m.groups():
            num_parts = int(m.group('num_parts'))
            if num_parts != 0:  # <num_parts> is 0 on the initial request before real size is discovered
                meta.num_parts = num_parts

        return s3_req

    def _post_processing(self):
        self.s3_run.max_time = self._line_time(self._last_line)

        # filter out threads that didn't host any HttpConnections
        self.s3_run.threads = {
            k: v for k, v in self.s3_run.threads.items() if len(v.connections) > 0}

        # for anything where we didn't find the end: snip it off
        snip_time = self.s3_run.max_time
        snip_error = '???'
        snip_error_num = -1

        for event_loop in self.s3_run.threads.values():
            for http_conn in event_loop.connections:
                if http_conn.end_time is None:
                    log(f"No end found for HttpConnection {http_conn.id}")
                    http_conn.end_time = snip_time
                    http_conn.error = snip_error

                for stream_idx, http_stream in enumerate(http_conn.streams):
                    if http_stream.end_time is None:
                        log(f"No end found for HttpStream {http_stream.id}")
                        assert stream_idx + 1 == len(http_conn.streams)
                        http_stream.end_time = snip_time
                        http_stream.error = snip_error

            self._determine_visual_indices(event_loop.connections)
            event_loop.visual_size = max(
                [conn.visual_idx for conn in event_loop.connections])

        for s3_meta in self.s3_run.meta_requests:
            if s3_meta.end_time is None:
                log(f"No end found for S3MetaRequest {s3_meta.id}")
                s3_meta.end_time = snip_time
                s3_meta.error_num = snip_error_num

            for s3_req in s3_meta.s3_requests_by_part_num.values():
                for attempt_idx, attempt in enumerate(s3_req.attempts):
                    if attempt.end_time is None:
                        # don't warn, nothing is logged if the request is abandoned
                        attempt.end_time = snip_time
                        attempt.error = snip_error

                if s3_req.end_time is None:
                    # don't warn, we don't currently have an S3_REQUEST_END log line
                    if s3_req.attempts:
                        last_attempt = s3_req.attempts[-1]
                        s3_req.end_time = last_attempt.end_time
                        s3_req.error = last_attempt.error
                    else:
                        s3_req.end_time = snip_time
                        s3_req.error = snip_error

            self._determine_visual_indices(
                s3_meta.s3_requests_by_part_num.values())

    def _determine_visual_indices(self, events: list):
        # find index for each event, such that we can use as few indices
        # as possible, without any any events overlapping each other
        indices = []
        for new_event in events:
            for idx, event in enumerate(indices):
                if event is None or event.end_time < new_event.start_time:
                    new_event.visual_idx = idx
                    indices[idx] = new_event
                    break

            if new_event.visual_idx is None:
                new_event.visual_idx = len(indices)
                indices.append(new_event)

    def _line_time(self, line) -> float:
        delta = line.date() - self.start_date
        return delta.total_seconds()


if __name__ == '__main__':
    args = ARG_PARSER.parse_args()

    with PerfTimer('scrape'):
        scraper = _Scraper(args.log_in)

    with PerfTimer('to-json'):
        json_data = asdict(scraper.s3_run)
        with open(args.json_out, 'w') as json_file:
            json.dump(json_data, json_file)
