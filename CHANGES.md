# Reportnet 3 changes

## 0.2.3
* Better labels for dataflow/datasets
* EEA specific contry codes

## 0.2.0
* Lot of changes under the hood... #136466, #145669, #145670

## 0.1.37
* 145209 country code reading

## 0.1.36
* Expose writer bulk size (#144438)

## 0.1.35
* Updated list of dataflow endpoints in reader:
   ```
   urls = [
    f'{self.base_url}/dataflow/getDataflows',
    f'{self.base_url}/dataflow/businessDataflows',
    f'{self.base_url}/dataflow/citizenScienceDataflows',
    f'{self.base_url}/dataflow/referenceDataflows'
   ]
   ```

* Updated list of dataset types in reader: ['reportingDatasets', 'designDatasets', 'dataCollections', 'euDatasets', 'referenceDatasets']
* Added session.close() in etl_import to allow long running imports.
* Versionified the web connection name. Old version of web connection will not work any more.

Issues:
* Reference dataflows are listed in the reader but cannot be opened yet.

## 0.1.34
* solves #144257 reading aggregates bug

## 0.1.33
* Attempt to fix #142976

## 0.1.32
* Fix writer bug in quick translator

## 0.1.31
* Solves writer bug where defline params could arrive in diffrent order.

## 0.1.30
* #142386 list writing

## 0.1.29
* Writer bug and some docs
* logging for geometry writing

## 0.1.28
* Geometry type handling in writer. writer bug should be fixed. #141687 #141450

## 0.1.27
* Fixes #141446. normal writer still has a nasty crash, use 0.1.25 for a more stable version.

## 0.1.26
* Option to disable geometry parsing, attempted option to allow integer as input for dataflow and dataset

## 0.1.25
* Geometry writing

## 0.1.24
* Fixes #140704

## 0.1.22
* Work on filtering - needs testing, should fix #140097 and #140098

## 0.1.21
* Limited filtering capabilities added, see comments: https://taskman.eionet.europa.eu/issues/139897

## 0.1.20
* provider_id from namedconnection, connectionstring as alternative to namedconnection

## 0.1.19
* Connection strings can now be embedded

## 0.1.18
* A lot of work on writer
* Started documentation of Reportnet3AttachmentDownloader

## 0.1.17
* Help now navigable
* Transformer attachmentdownloader functional

## 0.1.16
* Restored (hopefully) help content

## 0.1.15
* Attachment download transformer (not functional)

## 0.1.8
* Dynamic schema in workspace
* Logging configured (no c:\temp should show up)

## 0.1.5
* Restored lenient behaviour regarding value parsing and geom field selection

## 0.1.1
* Renaming of package and components
* Change of icon

## 0.1.0
* Initial version.

