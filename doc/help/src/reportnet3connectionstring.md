# Dynamic Connection String
In some cases it might be needed to dynamically configure the Reportnet3 connection.

The value supplied for the Reportnet3 connection will be evaluated like this:

1. If the value matches exactly an FME Named Connection, that will be used.
2. If the value is a URL in the form `<api_url>?API_KEY=<token>[&PROVIDER_ID=<provider_id>][&VERSION=<version>]`, that will be used.

Example of the second form:

`https://rn3api.eionet.europa.eu?VERSION=1&API_KEY=502982a2-95a5-43ae-bf3b-d16356042c86&PROVIDER_ID=5`

This logic is applied both on the reader, writer and the Reportnet3AttachmentDownloader.