# Reportnet 3 changes

## v1.1.1
* Four new data providers were added: Georgia (GE), Gibraltar (GI), Moldova (MD) and Ukraine (UA).

## v1.1.0
* Two new format attributes were added:
   * `reportnet_id_table_schema` - Id of the table schema
   * `reportnet_id_record` - Id of the record

## v1.0.3
* Verbose debugging can be controlled by using directives `EEA.REPORTNET.REPORTNET3_DEBUG_GEOMETRY` and `EEA.REPORTNET.REPORTNET3_DEBUG_HTTP_POST`. Example Workspace Header:
```
EEA.REPORTNET.REPORTNET3_DEBUG_GEOMETRY yes
EEA.REPORTNET.REPORTNET3_DEBUG_HTTP_POST C:\temp\my_rn3_http_posts
#! START_WB_HEADER
...
```

## v1.0.2
* "Bulk Size" parameter that controls how many FME Features is now a writer parameter instead of a feature type parameter
* Writer parameter "Connection Timeout" was added

## v1.0.1
README updated

## v1.0.0
Data provider code(s) can now be controlled by attribute

Errors when parsing geometry should now be more detailed

## v0.2.7

The setting for Dataflow ID has now moved into the connection parameters.

Because of this, the named connection has got a "version bump", i.e. named connections created with earlier versions of the reportnet3 package will not work with this version of the reader/writer/transformer.

Workspaces created with earlier versions of the reportnet3 package should continue to work.

## v0.2.6

Let Readers inherit name from the selected webconnection

## v0.2.5

Always use etlExport v2 (for now)

Minor fix in concurrent requests

## v0.2.4 Initial test release

First testrelease
