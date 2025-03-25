# Dynamic Connection String
In some cases it might be needed to dynamically configure the Reportnet3 connection.

The value supplied for the Reportnet3 connection will be evaluated like this:

1. If the value matches exactly an FME Named Connection, that will be used.
2. Else, if the value is a URL in one of the following forms, that will be used:

`<api_url>?API_KEY=<token>[&VERSION=1][&PROVIDER_ID=<provider_id>]`

`<api_url>?API_KEY=<token>&VERSION=2&DATAFLOW_ID=<dataflow_id>[&PROVIDER_ID=<provider_id>]`

`<api_url>?API_KEY=<token>&VERSION=3&DATAFLOW_ID=<dataflow_id>[&PROVIDER_ID=<provider_id>][&MAX_RETRIES=<max_retries>][&BACKOFF_FACTOR=<backoff_factor>][&RETRY_HTTP_CODES=<retry_http_codes>]`

`<api_url>?VERSION=3&API_KEY=<token>&DATAFLOW_ID=<dataflow_id>[&PROVIDER_ID=<provider_id>][&MAX_RETRIES=<max_retries>][&BACKOFF_FACTOR=<backoff_factor>][&RETRY_HTTP_CODES=<retry_http_codes>][&PAGING_LOGIC=OLD|NEW]`

Examples:
```
https://test-api.reportnet.europa.eu?API_KEY=502982a2-95a5-43ae-bf3b-d16356042c86

https://test-api.reportnet.europa.eu?VERSION=1&API_KEY=502982a2-95a5-43ae-bf3b-d16356042c86&PROVIDER_ID=5

https://test-api.reportnet.europa.eu?VERSION=2&API_KEY=502982a2-95a5-43ae-bf3b-d16356042c86&DATAFLOW_ID=861&PROVIDER_ID=10

https://test-api.reportnet.europa.eu?VERSION=3&API_KEY=502982a2-95a5-43ae-bf3b-d16356042c86&DATAFLOW_ID=861&PROVIDER_ID=10&MAX_RETRIES=3&BACKOFF_FACTOR=10&RETRY_HTTP_CODES=401,403

https://test-api.reportnet.europa.eu?VERSION=4&API_KEY=502982a2-95a5-43ae-bf3b-d16356042c86&DATAFLOW_ID=861&PROVIDER_ID=10&MAX_RETRIES=3&BACKOFF_FACTOR=10&RETRY_HTTP_CODES=401,403&PAGING_LOGIC=NEW
```

Please note that in version 2, `DATAFLOW_ID` was added as a mandatory url-query-parameter.

This logic is applied both on the reader, writer and the Reportnet3AttachmentDownloader.