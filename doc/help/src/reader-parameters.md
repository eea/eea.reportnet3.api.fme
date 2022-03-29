# Reportnet 3 Reader Parameters

### Reportnet 3 Connection
The Reportnet 3 Reader uses a Reportnet 3 Web Connection. The connection should be configured with an API-key from the e-Reporting platform.

### Dataflow
The Reportnet 3 *Dataflow*

### Dataset
The Reportnet 3 *Dataset*

### Table(s)
The Reportnet 3 *Table(s)* to read from the selected *Dataflow/Dataset*

## Advanced

### Bulk size
The number of features (table records) to fetch in each roundtrip to the Reportnet3 HTTP API backend.

Depending on how many fields a table has, the optimal value may differ.

Setting a value lower than `1` is not supported.

### Concurrent HTTP requests
Requires pagination (see above). If set, the reader will fetch pages in parallel up to the specified number of concurrent requests. In some scenarios this _may_ speed up throughput.

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

### Data Providers
For Reportnet3 Datasets that is composed from many data providers, this setting can be used to filter the read operation based on data provider code(s).

## Schema Attributes
### Additional Attributes to Expose
Use this parameter to expose Format Attributes in Workbench when you create a workspace:

* In a dynamic scenario, it means these attributes can be passed to the output dataset at runtime.
* In a non-dynamic scenario, this parameter allows you to expose additional attributes on multiple feature types. Click the browse button to view the available format attributes (which are different for each format) for the reader.


