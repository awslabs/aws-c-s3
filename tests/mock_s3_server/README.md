# Mock S3 server

A **NON-TLS** mock S3 server based on [python-hyper/h11](https://github.com/python-hyper/h11) and [trio](http://trio.readthedocs.io/en/latest/index.html). The server code implementation is based on the trio-server example from python-hyper/h11 [here](https://github.com/python-hyper/h11/blob/master/examples/trio-server.py). Only supports very basic mock response for request received.

## How to run the server

Python 3.7+ required.

- Install hyper/h11 and trio python module. `python3 -m pip install h11 trio`
- Run python. `python3 ./mock_s3_server.py`.

### Supported Operations

- CreateMultipartUpload
- CompleteMultipartUpload
- UploadPart
- AbortMultipartUpload
- GetObject

### Defined response

The server will read from ./{OperationName}/{Key}.json. The json file is formatted as following:

```json
{
    "status": 200,
    "headers": {"Connection": "close"},
    "body": [
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
        "",
        "<Error>",
         "<Code>InternalError</Code>",
         "<Message>We encountered an internal error. Please try again.</Message>",
         "<RequestId>656c76696e6727732072657175657374</RequestId>",
         "<HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>",
        "</Error>"
    ]
}
```

Where you can define the expected response status, header and response body. If the {Key}.json is not found from file system, it will load the `default.json`.

If the "delay" field is present, the response will be delayed by X seconds.

### GetObject Response

By default, the GetObject response will read from ./{OperationName}/{Key}.json for the status and headers. But the body will be generated to match the range in the request.

To proper handle ranged GetObject, you will need to modify the mock server code. Check function `handle_get_object` for details.
