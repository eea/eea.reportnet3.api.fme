# Reportnet 3 changes

## v1.1.0
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
