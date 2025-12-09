# File Queue Messaging System

A Go-based messaging system that uses a TCP queue service to transfer data between a file reader and writer asynchronously.

## Architecture

- A queue service that accepts TCP connections and offers Redis-like queue functionality.
- A service that runs the reader and writer simultaneously and uses the queue to copy the file contents.

## Testing

- Tests are writting in a BDD style to make the test more readable.
- You can run the tests normally with `go test ./... -v -race`

## Running the queue and file processes

- You can simply use `docker compose up`

### Notes

- The files are currently configured with an environment variable; this could be implemented more effectively as an input argument.
- The queue address and assigned port are currently hard-coded and can also be configured.
- Currently, there is no EOF detection implemented.
- Beyond scaling for thousands of files or very large files, considerations regarding I/O, network error recovery and retries are also necessary.
