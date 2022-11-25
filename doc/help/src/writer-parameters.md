# Reportnet 3 Writer Parameters

### Bulk Size
The parameter adjusts how many FME features will be bundled together for each HTTP transactions to the Reportnet 3 API. The parameter is required with a value larger than 0.

### Connection Timeout
Timeout value for the upload time.

If importing data to the Reportnet3 HTTP API backend is not finished within `timeout` seconds, the connection is aborted and an exception is raised.

The import is performed in chunks controlled by "Bulk Size". The timeout is set on each HTTP transaction.

For optimal performance, the "Bulk Size" and "Connection Timeout", may need to be adopted for different datasets.
