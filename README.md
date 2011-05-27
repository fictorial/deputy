# Deputy Job Server

A "client" can *do work* and *submit work* to be done by other clients.
When a client is doing work, we call it a "worker".

A client may submit multiple jobs before waiting for the result of
previous jobs. There is no enforced limit.

A worker is assigned at most one job at any given time. The idea is to
scale out the number of workers to handle more jobs. I might change this
in the near future to allow workers to concurrently do as many jobs as
they specify they are able to. This might be "better" for I/O bound
jobs.

Please only use this job server internally.

There is *no persistence* of job queues. When the server goes down for
any reason the queued jobs go down with it.

## Running a Server

    deputy-server [OPTIONS]

    -h <ip>      Listen on IP address (default: 127.0.0.1)
    -p <port>    Listen on port (default: 11746)
    -s <path>    Listen on a UNIX socket (no default)
    -v           Print basic metrics to stderr periodically (default: no)

## Protocol

Clients talk to a Deputy server by sending/receiving JSON over TCP/IP.

See [json-line-protocol](https://github.com/fictorial/json-line-protocol).

A worker advertises to the server what it can do to:

    { "cmd":"can_do", "types":["job-type", ...] }

A client submits a job, and the server tells a worker to do that job:

    { "cmd":"do", "id":"job-id", "type":"job-type", "arg":"value" }

A worker tells the server it's done with a job, and the server tells
the original client that their job is done.

    { "cmd":"did", "id":"job-id", "res":"value" }

A client requests that basic server metrics be returned:

    { "cmd":"metrics" }

Those metrics are returned:

    { "cmd":"metrics", "metrics":{ "up_since": ... } }

## Author

Brian Hammond <brian@fictorial.com> (http://fictorial.com)

## License

MIT
