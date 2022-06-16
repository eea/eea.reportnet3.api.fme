# Reportnet 3 changes

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
