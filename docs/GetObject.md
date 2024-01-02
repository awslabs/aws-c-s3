# GetObject

## Overview
The `GetObject` is used to download objects from Amazon S3. Optimized for throughput, the CRT S3 client enhances performance and reliability by parallelizing multiple part-sized `GetObject` with range requests.

## Flow Diagram
Below is the typical flow of a GetObject request made by the user.

![GetObject Flow Diagram](images/GetObjectFlow.svg)
