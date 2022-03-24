# Quick Facts


|  Keyword                      | Value                            |
| ----------------------------- | -------------------------------- |
| Format Type Identifier        | EEA.REPORTNET.REPORTNET3         |
| Requirements                  | FME Build 21222+                 |
| Reader/Writer                 | Reader                           |
| Licensing Level               | Professional and above           |
| Dependencies                  | Python 3.8+                      |
| Dataset Type                  | None                             |
| Feature Type                  | Table name                       |
| Typical File Extensions       | Not applicable                   |
| Automated Translation Support | Yes                              |
| User-Defined Attributes       | Yes                              |
| Coordinate System Support     | Yes                              |
| Generic Color Support         | No                               |
| Spatial Index                 | No                               |
| Schema Required               | Yes                              |
| Transaction Support           | No                               |
| Geometry Type                 | reportnet_type                   |
| Encoding Support              | UTF-8                            |


Geometry Support:

| Geometry       | Supported? |
| -------------- | ---------- |
| aggregate      | yes[^1]    |
| circles        | no         |
| circular arc   | no         |
| donut polygon  | yes        |
| elliptical arc | no         |
| ellipses       | no         |
| line           | yes        |
| none           | yes        |
| point          | yes        |
| polygon        | yes        |
| raster         | no         |
| solid          | no         |
| surface        | no         |
| text           | no         |
| z values       | yes        |

[^1]: Only homogeneous aggregates (MultiPoint, MultiLine...) but not heterogeneous ones.