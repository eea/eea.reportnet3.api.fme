# Dynamic Connection String
In some cases it might be needed to dynamically configure the Reportnet3 connection.

The value supplied for the Reportnet3 connection will be evaluated like this:

1. If the value matches exactly an FME Named Connection, that will be used.
2. Else, if the value is a URL in one of the following forms, that will be used:

`<api_url>?API_KEY=<token>[&VERSION=1][&PROVIDER_ID=<provider_id>]`

`<api_url>?API_KEY=<token>&VERSION=2&DATAFLOW_ID=<dataflow_id>[&PROVIDER_ID=<provider_id>]`


Examples:
```
https://test-api.reportnet.europa.eu?API_KEY=502982a2-95a5-43ae-bf3b-d16356042c86
https://test-api.reportnet.europa.eu?VERSION=1&API_KEY=502982a2-95a5-43ae-bf3b-d16356042c86&PROVIDER_ID=5
https://test-api.reportnet.europa.eu?VERSION=2&API_KEY=502982a2-95a5-43ae-bf3b-d16356042c86&DATAFLOW_ID=861&PROVIDER_ID=10
```

Please note that in version 2, `DATAFLOW_ID` was added as a mandatory url-query-parameter.

This logic is applied both on the reader, writer and the Reportnet3AttachmentDownloader.