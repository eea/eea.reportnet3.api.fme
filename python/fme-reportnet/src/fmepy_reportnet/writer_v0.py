from pluginbuilder import FMEWriter
from fmegeneral.plugins import FMEMappingFileWrapper
from fmegeneral.fmelog import get_configured_logger
from fmegeneral import fmeconstants
from .reader import Schema, ReportnetIdentifier
from fmegeneral.parsers import OpenParameters
import collections
from . import api as reportnet_api
from . import rn3_fme
import fmeobjects
from fmeobjects import FMECoordSysManager, FMEException
import json

MAX_CACHED_RECORDS = 10000
WriterParams = collections.namedtuple(
      'WriterParams'
    , [
          'connection'
        , 'dataflow'
        , 'dataset'
        , 'api_version'
        , 'reportnet_geom_column'
        , 'reportnet_writer_bulk_size'
    ]
)
DEFLINE_PARAMS = ['reportnet_type','reportnet_geom_column','reportnet_validate_geom_type', 'reportnet_writer_bulk_size']
def empty_log_cache():
    return {
          '_pt': {
              'msg': 'Writer recieved unexpected geometry type. Geometry type handling was set to "Pass through".',
              'count': 0
          }
        , '_df' : {
              'msg': 'Writer recieved unexpected geometry type. Geometry type handling was set to "Drop feature".',
              'count': 0
          }
        , '_no_geom' : {
              'msg': 'Writer could not determine target RN3 geometry column, the FME geometry was therefore ignored. Make sure either writer parameter "Spatial Column" or format attribute reportnet_geom_column is set.',
              'count': 0
          }
    }

GEOMETRY_MAPPING = {
      ('reportnet_point', fmeobjects.FME_GEOM_POINT): 'reportnet_point'
    , ('reportnet_linestring', fmeobjects.FME_GEOM_LINE): 'reportnet_linestring'
    , ('reportnet_polygon', fmeobjects.FME_GEOM_POLYGON): 'reportnet_polygon'
    , ('reportnet_polygon', fmeobjects.FME_GEOM_DONUT): 'reportnet_polygon'
    , ('reportnet_none',  fmeobjects.FME_GEOM_UNDEFINED): 'reportnet_none'
    , ('reportnet_point', fmeobjects.FME_GEOM_AGGREGATE): 'reportnet_multipoint'
    , ('reportnet_linestring', fmeobjects.FME_GEOM_AGGREGATE): 'reportnet_multilinestring'
    , ('reportnet_polygon', fmeobjects.FME_GEOM_AGGREGATE): 'reportnet_multipolygon'
}
def checkInt(str):
    try:
        int(str)
        return True
    except ValueError:
        return False
def parse_writer_param(k,s):
    if k in ('dataflow', 'dataset'):
        # attempt to allow integer as input. possible side effects?
        if checkInt(s):
            return ReportnetIdentifier('_', s)
        import re
        m = re.match(r'^(.*) \((\d+)\)$', s)
        if not m: return s
        return ReportnetIdentifier(*m.groups())
    return s

class Reportnet3Writer(FMEWriter):
    def __init__(self, writerType, destKeyword, mappingFile):
        self._logprefix = destKeyword
        self._mapping_file_wrapper = FMEMappingFileWrapper(mappingFile, destKeyword, writerType)

        self._logger = get_configured_logger(
              f'{self.__class__.__module__}.{self.__class__.__qualname__}'
            , mappingFile.fetch(fmeconstants.kFME_DEBUG) is not None #True
        )
        self._logger.debug('%s: %s', self._logprefix, 'Writer created')
        self._schemas = dict()
        self._cache = dict()
        self._cached_records = 0
        self.log_cache = empty_log_cache()
        self._client = None
        self._aborted = False
        self._params = None
    def _flush(self):
        data = {
            "tables": [
                {"tableName": k, "records": v}
                for k,v in self._cache.items()
                ]
            }
        self._logger.debug('%s: importing %s record(s)', self._logprefix, self._cached_records)
        #self._logger.debug('%s: %s', self._logprefix, data)
        self._client.etl_import(self._params.dataflow.id, self._params.dataset.id, data)
        self._cached_records = 0
        self._cache = dict()
    def _flush_log(self):
        for l in self.log_cache.values():
            self._logger.warn('%s: %s The message was repeated %s time(s)', self._logprefix, l['msg'], l['count']) if l['count'] > 0 else None
        self.log_cache = empty_log_cache()
    def abort(self):
        if self._client:
            self._client.abort()
        self._aborted = True
    def addMappingFileDefLine(self, defLine):
        """This method adds a _DEF line to the writer.
        For formats or systems which do not support schema from schema features, nothing is done.
        example_input = [
            'EEA.REPORTNET.REPORTNET3_1_DEF', 'd_y_n'
            , 'reportnet_type', 'reportnet_point'
            , 'reportnet_geom_column', 'geom'
            , 'reportnet_writer_bulk_size',
            , 'reportnet_validate_geom_type', 'PT' #Pass through
            , 'row_nbr', 'NUMBER_INTEGER'
            , 'geom_nbr', 'NUMBER_INTEGER'
            , 'part_nbr', 'NUMBER_INTEGER'
            , 'ring_nbr', 'NUMBER_INTEGER'
            , 'kind', 'CODELIST'
            , 'vertex_nbr', 'NUMBER_INTEGER'
        ]
        """
        # Between feature type and attribute types, there is a set of writer configuration 
        # params that we need to retrieve (see example_input above).
        # The order is not consequent so a dict helps assigning right value to right key
        # Feature type is always first
        pStart = 2
        pEnd = 2
        _, featuretype = defLine[:pStart]
        params = {el:None for el in DEFLINE_PARAMS}
        # The rest is assigned into dict for strict schema
        for i in range(pStart, len(defLine), 2):
            key = defLine[i]
            value = defLine[i + 1]
            if key in params.keys():
                pEnd += 2
                params[key] = value
        schema = Schema(featuretype)
        schema.geometry = (params['reportnet_geom_column'], params['reportnet_type'])
        schema.def_line_options = dict({
            'validate_geom_type': params['reportnet_validate_geom_type'] or 'PT', #Quick translator does not pass the value # TODO: params['missing key'] will raise KeyError - use params.get('missing key', 'fallback value') instead
            'writer_bulk_size': int((params['reportnet_writer_bulk_size'] or '10000').split('.')[0]) # Handle when 10.0 is allowed in FME Gui
        })
        if not params['reportnet_geom_column']:
            self._logger.warn('No geometry column specified for feature type. Will attempt to use format parameter reportnet_geom_column')
        schema.attribute_types.update(zip(*[iter(defLine[pEnd:])]*2))
        schema_keys = list(schema.attribute_types.keys())
        # Throw error if duplicate names between lists and attributes.
        # This scenario is allowed in FME but will cause RN3 api to write same attribute twice.
        if any(schema_keys.count(key) + schema_keys.count(key + '{}')  > 1 for key in schema_keys):
            raise FMEException('Detected ambigous attribute names in writer. Make sure there is no list "fieldName{}" that has the same name as an attribute "fieldName" ')
        #attributes = dict(zip(*[iter(defLine[4:])]*2)) # As of Python 3.7, regular dicts are guaranteed to be ordered
        self._logger.debug('%s: addMappingFileDefLine %s %s %s', self._logprefix, featuretype, params['reportnet_type'], schema)
        if featuretype in self._schemas:
            self._logger.warn('%s: Overwriting schema definition for feature type %s', self._logprefix, featuretype)
            self._logger.warn('%s: ...old schema was %s', self._logprefix, self._schemas[featuretype])
        self._schemas[featuretype] = schema
        pass
    def close(self):
        if not self._aborted:
            self._flush()
            self._flush_log()
        if self._client:
            self._client.close()
    def commitTransaction(self):
        pass
    def multiFileWriter(self):
        return False
    def open(self, datasetName, parameters):
        self._logger.debug('%s: open', self._logprefix)
        self._logger.debug('%s:   datasetName = `%s`', self._logprefix, datasetName)
        self._logger.debug('%s:    parameters = `%s`', self._logprefix, parameters)
        for defLine in self._mapping_file_wrapper.defLines():
            self.addMappingFileDefLine(defLine)
        if True: #not self._usingConstraints:
            self._search_envelope = self._mapping_file_wrapper.getSearchEnvelope()
            self._feature_types = self._mapping_file_wrapper.getFeatureTypes(parameters)
        parsed_parameters = OpenParameters(datasetName, parameters)
        self._logger.debug('%s: %s', self._logprefix, f'open\n\tsearch_envelope: {self._search_envelope}\n\tfeature_types: {self._feature_types}\n\tparsed parameters: {parsed_parameters}')
        merged_parameters = {
            f: v
            for f in
            WriterParams._fields
            for v in [
                parsed_parameters.get(f.upper(), self._mapping_file_wrapper.get(f'_REPORTNET_{f.upper()}'))
            ]
            if v is not None and len(v)
        }
        self._logger.debug('%s: merged parameters %s', self._logprefix, merged_parameters)
        credentials = rn3_fme.resolve_named_connection(merged_parameters.get('connection'))
        dataflow_id_in_credentials = getattr(credentials, 'DATAFLOW_ID', None)
        if dataflow_id_in_credentials is not None:
            merged_parameters['dataflow'] = dataflow_id_in_credentials
        self._params = WriterParams(*(
            parse_writer_param(f, merged_parameters.get(f))
            for f in
            WriterParams._fields
        ))
        self._logger.debug('%s: %s', self._logprefix, self._params)
        # TODO: In what situations would we *not* require a valid connection here?
        # if self._params.connection:

        # fetch value using getattr (falling back to '0') because it was added in version 1
        credentials_version = getattr(credentials, 'VERSION', '0')
        
        if (self._params.api_version, credentials_version) not in rn3_fme.VERSION_COMPATIBILITY_RN3API_WEBSVC:
            raise fmeobjects.FMEException('This Transformer uses version {} of the Repornet3 api but the supplied connection is of version {}.'.format(self._params.api_version, credentials_version))
        
        self._client = reportnet_api.create_client(
                self._params.api_version
            , credentials.API_KEY
            , base_url=credentials.API_URL
            , provider_id=credentials.PROVIDER_ID
            , timeout=60 # TODO: add to fmf - self._params.connection_timeout
            , log_name=f'{self.__class__.__module__}.{self.__class__.__qualname__}'
        )
        self._logger.debug('%s: Client created - %s', self._logprefix, self._client)
        #self._logger.debug('%s:      defLines = `%s`', self._logprefix, [*self._mapping_file_wrapper.defLines()])
    def rollbackTransaction(self):
        pass
    def startTransaction(self):
        pass
    def write(self, feature):
        featuretype = feature.getFeatureType()
        schema_name = feature.getAttribute('fme_template_feature_type') or featuretype
        schema = self._schemas.get(schema_name, None)
        if not schema:
            self._logger.warn('%s: No schema found for feature type %s', self._logprefix, schema_name)
            return

        # Reportnet type is handled by FME (specified in .fmf GEOM_MAP) and can be used here
        # However, it does not look like we can specify how homogenous collections (e.g. multipoint) should be mapped there
        # That's why we do a lookup based on both reportnet_type and feature.getGeometryType() below
        #self._logger.debug('reportnet_type: %s', feature.getAttribute('reportnet_type'))
        #self._logger.debug('geometry type: %s', feature.getGeometryType())

        # TODO: Connecting list attribute subcomponents in workbench to feature attributes ("gui attribute name mapping") does not work as expected.
        # Solution seems to be to use ListRenamer and "promote" these first...
        #self._logger.debug('attribute names: %s', feature.getAllAttributeNames())
        # Test geometry type before constructing json
        validate_geom_type = schema.def_line_options["validate_geom_type"]
        reportnet_type = GEOMETRY_MAPPING.get((feature.getAttribute('reportnet_type'), feature.getGeometryType()), '_no_mapping_found_')
        if schema.geometry[1] != reportnet_type:
            #terminate translation
            if validate_geom_type == 'TT':
                raise FMEException(f'Invalid geometry type. expected: {schema.geometry[1]} but recieved {reportnet_type}')
            #drop feature
            if validate_geom_type == 'DF':
                self.log_cache['_df']['count'] += 1
                return
            self.log_cache['_pt']['count'] += 1
        fields = [
            {
                  "fieldName": attr_name[:-len('{}')] if attr_name.endswith('{}') else attr_name
                , "value": encodeValue(feature.getAttribute(attr_name, list) if 'MULTISELECT_CODELIST' == attr_type else feature.getAttribute(attr_name, str))
            }
            for attr_name, attr_type in schema.attribute_types.items()
        ]
        if feature.hasGeometry():
            try:
                # Geometry column specifiead in writer feature type
                target_geom_column = schema.geometry[0] if schema.geometry[0] else feature.getAttribute('reportnet_geom_column')
                if target_geom_column:
                    feature.performFunction('@JSONGeometry(TO_ATTRIBUTE,GEOJSON,__temp_geom__)')
                    temp_geom = json.loads(feature.getAttribute('__temp_geom__'))
                    feature.removeAttribute('__temp_geom__')
                    epsg_code =  FMECoordSysManager().getCoordSysAsOGCDef(feature.getCoordSys(), 'EPSG')
                    geom = {
                        'type': 'Feature',
                        'geometry': temp_geom,
                        'properties': {
                            'srid': epsg_code
                        }
                    }
                    fields.append(
                        {
                            "fieldName": target_geom_column
                            , "value": json.dumps(geom,separators=(',', ':'))
                        })
                # Neither Spatial column or format parameter reportnet_geom_column was set
                else:
                    self.log_cache['_no_geom']['count'] += 1
            except Exception as e:
                raise FMEException(f'{e}')

        #self._logger.debug('%s: Writing feature type %s using schema %s', self._logprefix, featuretype, schema)
        record = {
            "countryCode": None,
            "fields": fields
        }
        #if self._cached_records and not self._cached_records % MAX_CACHED_RECORDS:
        if self._cached_records and not self._cached_records % schema.def_line_options['writer_bulk_size']:
            self._flush()
        if not featuretype in self._cache:
            self._cache[featuretype] = []
        self._cache[featuretype].append(record)
        self._cached_records += 1

def encodeValue(value):
    if type(value) is list:
        return ';'.join((str(x) for x in value))
    # 0 integers needs to be checked explicitly
    return str(value if value or value == 0 else "")

