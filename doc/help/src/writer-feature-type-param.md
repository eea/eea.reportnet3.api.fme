# Reportnet 3 Writer Feature Type Parameters
To access feature type parameters, click the gear icon on a feature type in the workspace. This opens the Feature Type Parameter Editor.

## Options
### Spatial Column
Determines target attribute in Reportnet 3 to store the FME geometry. If Spatial Column is not defined, the writer will attempt to use the format attribute `reportnet_geom_column` as fallback. If the FME feature has a geometry and neither Spatial Column nor `reportnet_geom_column` was present, the writer will ignore geometry writing and output a warning message.
### Geometry type handling
The parameter determines how strict the geometry type checking should be in the writer. For instance, if the writer geometry type was set to `reportnet_point` and a Polygon was recieved, the writer will act as following:
- Pass through: The geometry will be passed through to Reportnet 3 without type validation.
- Drop feature: The feature that contained the incorrect geometry type will be dropped.
- Terminate translation: The FME translation will terminate with an error message.
