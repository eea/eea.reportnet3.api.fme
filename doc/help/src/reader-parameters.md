# Reportnet 3 Reader Parameters

### Reportnet 3 Connection
The Reportnet 3 Reader uses a Reportnet 3 Web Connection. 

The connection should be configured with an API-key, a Dataflow ID and optionally Provider ID from the e-Reporting platform. 

The web connection also allows to configuration of Retry Failed Requests behaviour:
| Attribute Name            |  Description                                                                         |
| ------------------------- | ------------------------------------------------------------------------------- |
| Error Types to Retry             | This parameter allows users to select the types of errors which should lead to a retry attempt. The available options include connection errors such as a network timeout or dns failure, or various HTTP error codes in the 4xx-5xx range. |
| Maximum Retry Attempts (0-10)      | This parameter specifies the maximum number of retry attempts that will be made for a single feature, before that feature is output through the <Rejected> port of the transformer. |
| Backoff Factor     | This parameter specifies the amount of time that the transformer will wait before retrying a failed request. The timeout for each incremental retry will be calculated according to: `{backoff factor} * (2 ** ({number of previous retries}))` |


### Dataset
The Reportnet 3 *Dataset*

When selecting the Reportnet Dataset using the browse-button, the value will be encoded for readability, e.g. "My Dataset (42)", where 42 corresponds to the dataset id.

When supplying the value for dataset dynamically, such as when used in a FeatureReader, it is valid to only specify the numeric id.

### Table(s)
The Reportnet 3 *Table(s)* to read from the selected *Dataflow/Dataset*

## Advanced

### Bulk size
The number of features (table records) to fetch in each roundtrip to the Reportnet3 HTTP API backend.

Depending on how many fields a table has, the optimal value may differ.

Setting a value lower than `1` is not supported.

### Concurrent HTTP requests
The reader will fetch pages in parallel up to the specified number of concurrent requests. 

In some scenarios it _can_ be possible to increase the throughput by carefully configure the bulk size in combination with number of concurrent requests.

### Connection Timeout (seconds)
Timeout value for the *full* download time.

If retrieving data from the Reportnet3 HTTP API backend is not finished within `timeout` seconds, the connection is aborted and an exception is raised.

### Force data types
If set to `Yes`, FME will try to parse the text-values retrieved from the Reportnet3 HTTP API backend into the closest corresponding FME type, e.g. "5" NUMBER_INTEGER is parsed into an FME integer value of 5.

If set to `No`, interpretation is *not* performed and all values are passed to FME as text regardless of the declared type.

### Parse geometry
If set to `Yes`, FME will try to convert a geometry column to an FME geometry. The geometry column will be dropped from schema. The name of the geometry column will be stored in the format paramater `reportnet_geom_column`.

If set to `No`, FME will not attempt to create an FME geometry. All geometry columns from Reportnet 3 will maintain their json representation.

### Default geom column for all feature types
For Reportnet 3 tables that contain multiple geometry fields, this setting controls wich of them should be used by FME. The default behaviour is to use the first occuring geometry column in Reportnet 3 table definition.

## Schema Attributes
### Additional Attributes to Expose
Use this parameter to expose Format Attributes in Workbench when you create a workspace:

* In a dynamic scenario, it means these attributes can be passed to the output dataset at runtime.
* In a non-dynamic scenario, this parameter allows you to expose additional attributes on multiple feature types. Click the browse button to view the available format attributes (which are different for each format) for the reader.


