from re import T
import fmeobjects
from fmeobjects import FMEException, FMELogFile, FMEFeature
from pluginbuilder import FMEReader
import fmegeneral
from fmegeneral.plugins import FMEMappingFileWrapper
from fmegeneral.parsers import OpenParameters, parse_def_line
from fmegeneral.fmelog import get_configured_logger
from fmegeneral import fmeconstants, fmeutil
from . import api as reportnet_api
from . import rn3_fme
import fmewebservices
import json
from collections import namedtuple, OrderedDict
import sys
import requests

# just to understand the lifecycle of readers while developing we keep singleton counters:
# should be removed in production code
global REPORTNET3READER_COUNTER
REPORTNET3READER_COUNTER = 0

DO_PROFILE = False
ATTACHMENT_FIELD_SEP = ':'

COUNTRY_CODES = {'AL', 'AT', 'BE', 'BA', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'EL', 'HU', 'IS', 'IE', 'IT', 'XK', 'LV', 'LI', 'LT', 'LU', 'MT', 'ME', 'NL', 'MK', 'NO', 'PL', 'PT', 'RO', 'RS', 'SK', 'SI', 'ES', 'SE', 'CH', 'TR', 'UK'}

ReportnetIdentifier = namedtuple('ReportnetIdentifier', ['name', 'id'])

Params = namedtuple(
      'Params'
    , [
        'reportnet_connection'
        , 'reportnet_dataflow'
        , 'reportnet_dataset'
        , 'reader_bulk_size'
        , 'concurrent_http_requests'
        , 'force_data_types'
        , 'geometry_column'
        , 'geometry_handling'
        , 'connection_timeout'
        , 'max_features'
        , 'max_features_per_feature_type'
        , 'start_feature_num'
        , 'min_features'
        , 'mode'
        , 'retrieve_all_table_names'
        , 'reportnet_api_version'
        , 'data_provider_codes'
    ]
    , defaults=[None,None,None,15000,1,None,None,None,None,None,None,None,None,None,None,'1',None]
)
Attribute = namedtuple(
    'Attribute'
    , [
        'reportnet_attr_type'
        , 'fme_name'
        ]
)
def checkInt(str):
    try:
        int(str)
        return True
    except ValueError:
        return False
def parse_param(k,s):
    if s is None: return None
    if not len(s): return None
    if 'connection_timeout' == k:
        return float(s)
    if k in ('reportnet_dataflow', 'reportnet_dataset'):
        # attempt to allow integer as input. possible side effects?
        if checkInt(s):
            return ReportnetIdentifier('_', s)
        import re
        m = re.match(r'^(.*) \((\d+)\)$', s)
        if not m: return s
        return ReportnetIdentifier(*m.groups())
    if k in ('reader_bulk_size', 'concurrent_http_requests', 'max_features', 'max_features_per_feature_type', 'start_feature_num', 'min_features'):
        return int(s)
    if 'data_provider_codes' == k:
        supplied_codes = {*s.split()}
        if not len(supplied_codes):
            return None
        valid_codes = {c for c in supplied_codes if c in COUNTRY_CODES}
        non_valid = supplied_codes - valid_codes
        if len(non_valid):
            raise FMEException(f'The supplied data provider code(s) {non_valid} could be validated')
        return ','.join(valid_codes)
    if type(s) in (list, tuple):
        # This was added as a workaround for a strange case where we retrieve duplicated key-value-pairs in open parameters for reportnet_api_version
        unique_values_as_list = [*{*s}]
        if 1 == len(unique_values_as_list):
            return unique_values_as_list[0]
        raise FMEException(f'Multiple values ({s}) were supllied for parameter {k}')
    return s
def parse_where_clause(s):
    import sqlparse
    import urllib.parse
    # url decode
    s = urllib.parse.unquote(s)
    if not s: return {'columnName': '', 'filterValue': ''}
    try:
        identifier, operand, value = (t
            for s in sqlparse.parse(s)
            for t in s.flatten()
            if tuple(t.ttype) not in [('Text', 'Whitespace'), ('Text', 'Whitespace', 'Newline'), ('Comment', 'Multiline'), ('Comment', 'Single')]
        )
        # Additional checks for token types and operand
        if identifier.ttype[0] not in ('Literal', 'Name') or operand.value != '=' or value.ttype[0] != 'Literal':
            raise Exception()

        # columnName is safe to strip. it should not contain any quotes after stripping.
        columnName = identifier.value.strip('"')
        filterValue = value.value
        if '"' in columnName or "'" in columnName:
            raise Exception()

        # filterValue should start with ' or nothing. startswith is enough since sqlparse will except unbalanced quotes
        if filterValue.startswith('"'):
            raise Exception()
        elif filterValue.startswith("'"):
            filterValue = filterValue[1:-1]


        # retrieve remaining quotes
        positions = [i for i,c in enumerate(filterValue) if "'" == c]
        # Check if quotes unbalanced
        if len(positions) % 2: raise Exception()
        # Check if quotes adjacent
        if [(a,b) for a,b in zip(*[(i for i in positions)]*2) if b-a != 1]: raise Exception()
        return {'columnName': columnName, 'filterValue': filterValue}

    except:
        raise FMEException(f'Could not parse where clause: \n`{s}`\nPlease ensure that the syntax is \"table_column\" = \'filter_value\'')


GEOMETRY_TYPES = ['POINT', 'LINESTRING','POLYGON', 'MULTIPOINT', 'MULTILINESTRING', 'MULTIPOLYGON']
VALUE_PARSERS = {
      'NUMBER_INTEGER': int
    , 'NUMBER_DECIMAL': lambda s: float(s.replace(',','.'))
    , 'MULTISELECT_CODELIST': lambda s: list(map(str.strip, s.split(';')))
}

VALUE_NULLS = {
     'TEXT': fmeobjects.FME_ATTR_STRING
   , 'NUMBER_INTEGER': fmeobjects.FME_ATTR_INT32
   , 'NUMBER_DECIMAL': fmeobjects.FME_ATTR_REAL64
   , 'MULTISELECT_CODELIST': fmeobjects.FME_ATTR_STRING
}

def to_fme_exception(e, params):
    # ("HTTP request to {} resulted in status code: {} \n {}".format(url, resp.status_code, resp.text))
    msg = []
    if isinstance(e, requests.exceptions.RequestException):
        msg.append(str(e.__class__))
        if e.request:
            msg.append(f'HTTP request to {e.request.url} failed.')
        if isinstance(e, requests.exceptions.ReadTimeout):
             msg.append(f'A read timeout occured, consider increasing the value of parameter "Connection Timeout", current value is {params.connection_timeout} second(s)')
        if e.response:
            msg.append(f'HTTP status code: {e.response.status_code}')
            msg.append(f'HTTP response text: {e.response.text}')
    if not len(msg):
        msg.append(str(e))
    return FMEException('\n'.join(msg))
class Schema(object):
    def __init__(self, name):
        self.name = name
        self.id = None
        self.attribute_types = OrderedDict()
        self.geometry = (None, 'reportnet_none') # (geom field name, reportnet_type)
        self.def_line_attributes = None
        self.def_line_options = None
        self.constraints = dict()
    def adopt_where_clause(self,parsed):
        try:
            if not parsed.get('columnName', None) in self.attribute_types:
                return False
            verified = {'columnName': parsed['columnName']}
            # filterValue should always be passed to us as a str, see parse_where_clause above
            # TODO: It could really be relevant to improve validation rules here and be more strict.
            v = parsed.get('filterValue', None)
            if v is not None:
                attr = self.attribute_types[parsed['columnName']]
                if str == type(v) and attr.reportnet_attr_type in ['ATTACHMENT', 'CODELIST', 'DATETIME', 'EMAIL', 'PHONE', 'LINK', 'TEXT', 'TEXTAREA', 'URL']:
                    verified['filterValue'] = v
                elif str == type(v) and 'NUMBER_INTEGER' == attr.reportnet_attr_type:
                    parsed_value = VALUE_PARSERS['NUMBER_INTEGER'](v)
                    if not str(parsed_value) == v:
                        return False
                    verified['filterValue'] = v
                elif str == type(v) and attr.reportnet_attr_type in VALUE_PARSERS:
                    # TODO: for NUMBER_DECIMAL and MULTISELECT_CODELIST we could improve our effort to validate user input here
                    _ = VALUE_PARSERS[attr.reportnet_attr_type](v)
                    verified['filterValue'] = v
                else:
                    return False
            self.constraints['where'] = verified
            return True
        except:
            return False
    def __str__(self):
        return f'Schema({self.name}){{\n\tid: {self.id}\n\tattribute_types:{self.attribute_types}\n\tgeometry:{self.geometry}\n\tdef_line_attributes:{self.def_line_attributes}\n\tdef_line_options:{self.def_line_options}\n\tconstraints:{self.constraints}\n}}'
    def __repr__(self):
        return self.__str__()

class Reportnet3Reader(FMEReader):
    def __init__(self, readerTypeName, readerKeyword, mappingFile):

        if DO_PROFILE:
            import cProfile
            self._profiler = cProfile.Profile()
            self._profiler.enable()

        self._mapping_file_wrapper = FMEMappingFileWrapper(mappingFile, readerKeyword, readerTypeName)

        self._logger = get_configured_logger(
              f'{self.__class__.__module__}.{self.__class__.__qualname__}'
            , mappingFile.fetch(fmeconstants.kFME_DEBUG) is not None #True
        )

        global REPORTNET3READER_COUNTER
        self._log_prefix = f'{readerKeyword}#{REPORTNET3READER_COUNTER}'
        REPORTNET3READER_COUNTER += 1
        self._params = None
        self._read_iterator = None
        self._schemas = None
        self._reportnet_schemas = None
        self._reportnet_schema_ids = None
        self._reportnet_client = None
        self._search_envelope = None
        self._feature_types = None
        self._aborted = False
        self._global_constraints = dict()
        self._constraints = dict()

        # A counter for keeping track of the order of calls to our code
        self._debug_counter = 0
        self._debug(f"Reader created, {sys.implementation}")

    def _debug(self, msg):
        self._logger.debug('%s [%d]: %s', self._log_prefix, self._debug_counter, msg)
    def _info(self, msg):
        self._logger.info('%s: %s', self._log_prefix, msg)
    def _warn(self, msg):
        self._logger.warn('%s: %s', self._log_prefix, msg)
    def _error(self, msg):
        self._logger.error('%s: %s', self._log_prefix, msg)
    def _critical(self, msg):
        self._logger.critical('%s: %s', self._log_prefix, msg)


    def _create_features_iterator(self):
        dataflow = self._params.reportnet_dataflow
        if not isinstance(dataflow, ReportnetIdentifier):
            raise FMEException(f'Dataflow {dataflow} has missing or invalid ID')
        dataset = self._params.reportnet_dataset
        if not isinstance(dataset, ReportnetIdentifier):
            raise FMEException(f'Dataset `{dataset}` has missing or invalid ID')

        """
        Generator of data features for selected feature types.
        """
        if not self._schemas:
            self._init_schemas(dataflow.id, dataset.id, True)

        force_data_types = 'Yes' == self._params.force_data_types
        geometry_handling = 'Yes' == self._params.geometry_handling
        total_features_read = 0
        # Currently no option in api to skip X features.
        # Therefore need to add Start Feature value to Max Features.
        start_feature_num = self._params.start_feature_num or 0
        max_features = self._params.max_features or 0
        max_features_per_feature_type = self._params.max_features_per_feature_type or 0
        skipped_features = 0
        for featuretype, schema in self._schemas.items():
            features_read = 0
            limit = None

            # Below is logic for:
            # - Start feature
            # - Max features
            # - Max features per feature type
            # Need to handle each combination of these.
            # More complicated if start feature is used.
            if start_feature_num and not (max_features or max_features_per_feature_type):
                # Do nothing
                pass
            elif start_feature_num and max_features and max_features_per_feature_type:
                limit = min(max_features - (total_features_read - skipped_features), max_features_per_feature_type) + start_feature_num - skipped_features
            elif start_feature_num and max_features:
                limit = max_features + start_feature_num - total_features_read
            elif start_feature_num and max_features_per_feature_type:
                limit = max_features_per_feature_type + start_feature_num
            # IF start features is not used:
            elif max_features and max_features_per_feature_type:
                limit = min(max_features_per_feature_type, max_features - total_features_read)
            elif max_features:
                limit = max_features - total_features_read
            elif max_features_per_feature_type:
                limit = max_features_per_feature_type

            #concurrent_http_requests = None
            #elt_export = self._reportnet_client.etl_export
            # TODO: could be same etl_export either way
            # If concurrent features is enabled in reader, use diffrent etl_export-function (for now)
            #if self._params.concurrent_http_requests and self._params.reader_bulk_size:
            #    elt_export = self._reportnet_client.init_concurrent_etl_export
            #    concurrent_http_requests = self._params.concurrent_http_requests
            for row in self._reportnet_client.etl_export(dataflow.id, dataset.id, schema.id, 
                    limit=limit, 
                    concurrent_http_requests=self._params.concurrent_http_requests, 
                    filter=schema.constraints.get('where', {}), 
                    data_provider_codes=self._params.data_provider_codes, 
                    ordered=True):
                # FME will automatically ignore X first features if "Start feature" is selected.
                # Need to keep track of this number for paging purposes.
                if skipped_features < start_feature_num:
                    skipped_features += 1
                feature = FMEFeature()
                feature.setFeatureType(featuretype)
                feature.setGeometry(fmeobjects.FMENull())  # Automatically sets fme_type attr.
                feature.setAttribute("reportnet_type", "reportnet_none")

                geom_field_name, geom_field_type = schema.geometry
                feature.setAttribute("reportnet_geom_column", geom_field_name)
                feature.setAttribute("reportnet_country_code", row["countryCode"] or '')

                # Set attributes onto the feature.
                # As the metafile defines ATTRIBUTE_READING to ALL,
                # this simply sets all available attributes onto the feature.
                for field in row["fields"]:
                    name, value, field_value_id = field["fieldName"], field["value"], field["field_value_id"]
                    try:
                        if geom_field_name and geom_field_name == name:
                            # TODO: introduce invalid geometry handling concept - reader parameter warn/reject/...
                            geom_json = json.loads(value)
                            if not isinstance(geom_json, dict):
                                raise Exception(f'Could not extract geometry from value `{geom_json}` of type `{type(geom_json)}`')
                            srid = geom_json.get('properties', {}).get('srid')
                            # sometimes json contains only geometry,
                            # sometimes it is a full geojson
                            if "geometry" in geom_json:
                                geom_json = geom_json['geometry']
                            feature.setAttribute('__geom_json__', json.dumps(geom_json))
                            feature.performFunction('@JSONGeometry(FROM_ATTRIBUTE,GEOJSON,__geom_json__)')
                            feature.removeAttribute('__geom_json__')
                            feature.setAttribute("reportnet_type", geom_field_type)
                            if srid:
                                # Hoping that srid is an FME recognizable epsg code
                                feature.setCoordSys(f'EPSG:{srid}')
                            continue
                        elif name not in schema.attribute_types:
                            self._warn(f'Unexpected field `{name}` encountered')
                            continue
                        elif value is None or not value:
                            # Nulls need to be set differently.
                            # Map type of null value to known data types, use FME_ATTR_STRING as fallback
                            attr_alias = schema.attribute_types[name].fme_name
                            attr_type = schema.attribute_types[name].reportnet_attr_type
                            nullType = VALUE_NULLS.get(attr_type) or fmeobjects.FME_ATTR_STRING
                            feature.setAttributeNullWithType(attr_alias, nullType)
                            continue
                        reportnet_attr_type = schema.attribute_types[name].reportnet_attr_type
                        if 'ATTACHMENT' == reportnet_attr_type and field_value_id:
                            value = ATTACHMENT_FIELD_SEP.join([value, dataflow.id, dataset.id, field_value_id])
                        if force_data_types and reportnet_attr_type in VALUE_PARSERS:
                            # TODO: reject features that fail?
                            value = VALUE_PARSERS[reportnet_attr_type](value)
                        feature.setAttribute(schema.attribute_types[name].fme_name, value)
                    except Exception as e:
                        self._error(e)
                        feature.setAttribute(f'{name}.error', str(e))
                        feature.setAttributeNullWithType(name, fmeobjects.FME_ATTR_STRING)
                yield feature
                features_read += 1
            total_features_read += features_read

    def _create_schema_iterator(self):
        self._debug(f'create_schema_iterator {self._params}')
        if 'DATAFLOWS' == self._params.mode:
            for name,id in self._reportnet_client.dataflows():
                feature = FMEFeature()
                feature.setFeatureType(f'{name} ({id})')
                yield feature
            return
        dataflow = self._params.reportnet_dataflow
        if not isinstance(dataflow, ReportnetIdentifier):
            raise FMEException(f'Dataflow {dataflow} has missing or invalid ID')
        if 'DATASETS' == self._params.mode:
            for name,id in self._reportnet_client.dataflow_datasets(dataflow.id):
                feature = FMEFeature()
                feature.setFeatureType(f'{name} ({id})')
                yield feature
            return
        dataset = self._params.reportnet_dataset
        if not isinstance(dataset, ReportnetIdentifier):
            raise FMEException(f'Dataset `{dataset}` has missing or invalid ID')
        if self._params.retrieve_all_table_names:
            # RETRIEVE_ALL_TABLE_NAMES is automatically added by the FEATURE_TYPES GUI type.
            for name,id in self._reportnet_client.table_schema_ids(dataflow.id, dataset.id):
                feature = FMEFeature()
                feature.setFeatureType(name)
                yield feature
            return
        if 'GEOMETRY_COLUMNS' == self._params.mode:
            # List all geometry columns in schema
            geom_fields = []
            self._warn('doing geom stuff')
            for tbl in self._reportnet_client.simple_schema(dataflow.id, dataset.id):
                self._warn("{}".format(tbl))

                all_fields = tbl["fields"]
                # Retrieve geoemtry columns of all tables in schema. At this point user has not selected table yet (feature_type)
                #tbl_geom_fields = list(filter(lambda d: d['fieldName'] if (d['fieldType'] in GEOMETRY_TYPES and d['fieldName'] not in geom_fields) else None, all_fields)) or []
                tbl_geom_fields = [d['fieldName'] for d in all_fields if d['fieldType']
                                   in GEOMETRY_TYPES and d['fieldName'] not in geom_fields]
                self._warn("{}".format(tbl_geom_fields))
                geom_fields.extend(tbl_geom_fields)

            self._warn("{}".format(geom_fields))
            for name in geom_fields:
                feature = FMEFeature()
                feature.setFeatureType(name)
                yield feature
            return
        if not self._schemas:
            self._init_schemas(dataflow.id, dataset.id)
        for featuretype, schema in self._schemas.items():
            feature = FMEFeature()
            feature.setFeatureType(featuretype)
            # fme_name may map between attr and attr{}
            for name, (reportnet_attr_type, fme_name) in schema.attribute_types.items():
                feature.setSequencedAttribute(fme_name, reportnet_attr_type)

            # fme_geometry is required and must be a value from metafile's GEOM_MAP.
            feature.setAttribute("fme_geometry{}", [schema.geometry[1]])
            yield feature

    def _init_schemas(self, dataflow_id, dataset_id, get_schema_ids = False):
        self._debug('_init_schemas(self, dataflow_id, dataset_id, get_schema_ids = False)')
        if not self._reportnet_schemas:
            # We're caching the api response because _init_schemas may be called multiple times if we're invoked from a FeatureReader
            self._reportnet_schemas = list(self._reportnet_client.simple_schema(dataflow_id, dataset_id))
        self._schemas = {}
        for table in self._reportnet_schemas:
            featuretype = table["tableName"]
            # _featureTypes is automatically filled with selected feature types, see open(self).
            # Only return schema features for selected feature types.
            # FME convention: If no feature types are selected, then return schema features for all.
            if self._feature_types and featuretype not in self._feature_types:
                continue
            self._schemas[featuretype] = Schema(featuretype)
            schema = self._schemas[featuretype]
            available_geom_fields = [f["fieldName"] for f in table["fields"] if f["fieldType"] in GEOMETRY_TYPES]
            choosen_geom_field = None
            if 'No' == self._params.geometry_handling:
                self._warn('Parse geometry was deselected. Skipping geometry!')
            elif self._params.geometry_column and self._params.geometry_column in available_geom_fields:
                choosen_geom_field = self._params.geometry_column
            elif len(available_geom_fields):
                choosen_geom_field = available_geom_fields[0]
            if self._params.geometry_column and self._params.geometry_column != choosen_geom_field:
                self._error("Did not find geom field {} in {}, using {} instead".format(
                    self._params.geometry_column, featuretype,  choosen_geom_field))
            for field in table["fields"]:
                # Create FME List if native data type was multiselection
                fme_field_name = field["fieldName"]
                if 'Yes' == self._params.force_data_types and field["fieldType"] == 'MULTISELECT_CODELIST':
                    fme_field_name = field["fieldName"] + "{}"

                field_name = field["fieldName"]
                field_type = field["fieldType"]
                if 'Yes' == self._params.geometry_handling and choosen_geom_field and field_name == choosen_geom_field:
                    schema.geometry = (field_name, f'reportnet_{field_type.lower()}')
                else:
                    attr = Attribute(reportnet_attr_type=field_type, fme_name=fme_field_name)
                    schema.attribute_types[field_name] = attr

            if self._global_constraints or featuretype in self._constraints:
                #self._debug('  self._global_constraints: {}'.format(self._global_constraints))
                #self._debug('  self._constraints: {}'.format(self._constraints))
                constraints = {**self._global_constraints, **self._constraints.get(featuretype, {})}
                self._debug('Trying to apply constraints {} to feature type {}'.format(constraints, featuretype))
                if 'where' in constraints:
                    where_clause = constraints['where']
                    self._debug(f'Adding where clause {where_clause} to featuretype {featuretype} from reader constraints')
                    if not schema.adopt_where_clause(where_clause):
                        self._warn(f'Where clause `{where_clause}` ignored for feature type `{featuretype}`. All Features will be read.')

        self._debug('Parsing mapping file def-lines')
        for defline in self._mapping_file_wrapper.defLines():
            feature_type, attributes, options = parse_def_line(defline, ['fme_attribute_reading', 'reportnet_where_clause'])
            if not feature_type in self._schemas: continue
            self._schemas[feature_type].def_line_attributes = attributes
            self._schemas[feature_type].def_line_options = options
            if 'reportnet_where_clause' in options:
                where_clause = parse_where_clause(options['reportnet_where_clause'])
                if not self._schemas[feature_type].adopt_where_clause(where_clause):
                    self._warn(f'Where clause `{where_clause}` ignored for feature type `{feature_type}`. All Features will be read.')


            # Attribute reading - when set to "Exposed", remove those that are not exposed
            # Note: This has minimal effect because there is no way to read partitial content
            # from the endpoint api. Left here for reference only.
            if not 'defined' == options.get('fme_attribute_reading', None): continue
            remove_us = [k for k, v in self._schemas[feature_type].attribute_types.items() if v.fme_name not in attributes]
            for x in remove_us:
                removed = self._schemas[feature_type].attribute_types.pop(x)
                self._debug(f'Removed attribute {removed} from schema')

        # schema id:s are not needed when the reader is used for listing table names
        if not get_schema_ids:
            self._debug('*NOT* fetching schema id:s')
            self._debug(self._schemas)
            return
        if not self._reportnet_schema_ids:
            self._debug('Fetching schema id:s')
            self._reportnet_schema_ids = list(self._reportnet_client.table_schema_ids(dataflow_id, dataset_id))
        for name, id in self._reportnet_schema_ids:
            if not name in self._schemas: continue
            self._schemas[name].id = id
        self._debug(self._schemas)

    def abort(self):
        self._debug_counter += 1
        self._debug('abort')
        self._aborted = True

    def close(self):
        self._debug_counter += 1
        self._debug('close')
        if self._read_iterator:
            self._read_iterator.close()
        if DO_PROFILE:
            import pstats, io
            from pstats import SortKey
            # ... do something ...
            self._profiler.disable()
            s = io.StringIO()
            sortby = SortKey.CUMULATIVE
            ps = pstats.Stats(self._profiler, stream=s).sort_stats(sortby)
            ps.print_stats()
            self._debug(s.getvalue())

    """The response should describe what is relevant for FME to communicate later when calling setConstraints.
    The logic here have been quite hard to understand.
    In order for FeatureReader to be able to use where-clause we have to
       1. Support fme_search_type fme_all_features
       2. fme_all_features needs to support both fme_where *and* fme_feature_type
    """
    def getProperties(self, propertyCategory):
        self._debug_counter += 1
        self._debug(f'getProperties {propertyCategory}')
        return {
                'fme_search_type': [
                      'fme_search_type', 'fme_all_features'
                ]
                , 'fme_all_features': [
                      'fme_all_features', 'fme_where'
                    , 'fme_all_features', 'fme_feature_type'
                ]
            }.get(propertyCategory, None)

    def open(self, datasetName, parameters):
        self._debug_counter += 1
        self._debug(f'open\n\tdatasetName: {datasetName}\n\tparameters: {parameters}')
        if True: #not self._usingConstraints:
            self._search_envelope = self._mapping_file_wrapper.getSearchEnvelope()
            self._feature_types = self._mapping_file_wrapper.getFeatureTypes(parameters)
        parsed_parameters = OpenParameters(datasetName, parameters)
        self._debug(f'search_envelope: {self._search_envelope}\n\tfeature_types: {self._feature_types}\n\tparsed parameters: {parsed_parameters}')
        self._params = Params(**{k:v for k,v in (
            (f, parse_param(f, parsed_parameters.get(f.upper(), self._mapping_file_wrapper.get(f'_{f.upper()}'))) )
            for f in
            Params._fields
        ) if v is not None})
        self._debug(f'open parameters merged with mapping file parameters: {self._params}')
        if self._params.reader_bulk_size is None or self._params.reader_bulk_size < 1:
            raise FMEException("Reader Bulk size has to be set to a value greater than 0")
        credentials = rn3_fme.resolve_named_connection(self._params.reportnet_connection)


        # Use paging anyway, if user selected max_features
        #paging = None
        # no point using features_per_fetch if value is 0, use
        #features_per_fetch = self._params.features_per_fetch if self._params.features_per_fetch else None
        #min_compare = [features_per_fetch, self._params.max_features, self._params.max_features_per_feature_type]
        #min_compare = [features_per_fetch]
        #min_compare = [i for i in min_compare if i is not None]
        #if min_compare:
        #    paging = min(min_compare)
        paging = self._params.reader_bulk_size if self._params.reader_bulk_size else None

        # fetch value using getattr (falling back to '0') because it was added in version 1
        credentials_version = getattr(credentials, 'VERSION', '0')

        if self._params.reportnet_api_version != credentials_version:
            raise FMEException('This Workspace/MappingFile was created using version {} of the Repornet3 api but the supplied connection is of version {}.'.format(self._params.reportnet_api_version, credentials_version))

        self._reportnet_client = reportnet_api.create_client(
              self._params.reportnet_api_version
            , credentials.API_KEY
            , base_url=credentials.API_URL
            , provider_id=credentials.PROVIDER_ID
            , timeout=self._params.connection_timeout
            , paging=paging
            , log_name=f'{self.__class__.__module__}.{self.__class__.__qualname__}'
        )
        self._debug(f'Client created: {self._reportnet_client}')

    def read(self):
        self._debug_counter += 1
        #self._debug('read(self) called')
        try:
            if not self._read_iterator:
                self._read_iterator = self._create_features_iterator()
            return next(self._read_iterator, None)
        except Exception as e:
            self._error(f'class: {e.__class__}, e:\n{e}')
            raise to_fme_exception(e, self._params)
            #passif resp.status_code != status_codes.ok:
            #raise FMEException("HTTP request to {} resulted in status code: {} \n {}".format(url, resp.status_code, resp.text))


    def readSchema(self):
        self._debug_counter += 1
        self._debug('readSchema(self) called')
        try:
            self._debug(f'readSchema, mode: {self._params.mode}')
            if not self._read_iterator:
                self._read_iterator = self._create_schema_iterator()
            return next(self._read_iterator, None)
        except Exception as e:
            self._error(f'class: {e.__class__}, e:\n{e}')
            raise to_fme_exception(e, self._params)

    def setConstraints(self, feature):
        self._debug_counter += 1
        # This may be called multiple times
        self._debug('setConstraints(self, feature) called')
        self._debug(" Constraint Feature:")
        for attr in feature.getAllAttributeNames():
            val = '<missing>'
            if not feature.isAttributeMissing(attr):
                if feature.isAttributeNull(attr):
                    val = '<null>'
                else:
                    val = feature.getAttribute(attr)
                self._debug('  {} ({}): `{}`'.format(attr, feature.getAttributeType(attr), val))
            else:
                self._debug('  {} : `{}`'.format(attr, val))

        where_clause = parse_where_clause(feature.getAttribute('fme_where'))
        if not where_clause:
            self._warn('No where clause specified')
            return

        feature_types = feature.getAttribute('fme_feature_type{}')
        if feature_types is None:
            self._warn('Setting a global where clause may cause unexpected behaviour, not all feature types may support the same where clause')
            self._global_constraints['where'] = where_clause
        else:
            for feature_type in feature_types:
                if feature_type not in self._constraints:
                    self._constraints[feature_type] = dict()
                self._constraints[feature_type]['where'] = where_clause
        # Resetting - WHY???
        self._read_iterator = None
        self._schemas = None


    def spatialEnabled(self):
        self._debug_counter += 1
        self._debug('spatialEnabled()')
        return True

