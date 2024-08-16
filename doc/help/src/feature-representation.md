# Reportnet 3 Feature Representation (Format Attributes)

| Attribute Name            | Notes     | Contents                                                                         |
| ------------------------- | --------- | -------------------------------------------------------------------------------- |
| reportnet_type            | Read-only | `reportnet_none`, `reportnet_point`, `reportnet_linestring`, `reportnet_polygon` |
| reportnet_geom_column     | Read-only | The Reportnet3 field that was used to create FME geometry.                       |
| reportnet_country_code    | Read-only | The reporting country (when applicable).                                         |
| reportnet_id_table_schema | Read-only | Id of the table schema (when applicable).                                        |
| reportnet_id_record       | Read-only | Id of the record (when applicable).                                              |
