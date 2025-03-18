import fme
import fmeobjects
from collections import namedtuple
from . import api as reportnet_api
from fmegeneral.fmelog import get_configured_logger
from fmegeneral.fmeconstants import kFME_REJECTION_CODE, kFME_REJECTION_MESSAGE
import io
import os
import os.path
import TransformerUtil
from . import rn3_fme
XFORMER_API_VERSION_MAP = {1:'0', 2:'1', 3:'2'}
XFORMER_PARAMS_VERSION_MAP = {
    1: namedtuple('ParamsV1', [
              'reportnet_connection'
            , 'src_attr'
            , 'src_attr_single'
            , 'src_attr_multiple'
            , 'save_file'
            , 'target_attr'
            , 'target_attr_encoding'
            , 'output_dirname'
            , 'timeout'
        ]
    ),
    2: namedtuple('ParamsV2', [
              'reportnet_connection'
            , 'src_attr'
            , 'src_attr_single'
            , 'src_attr_multiple'
            , 'save_file'
            , 'target_attr'
            , 'target_attr_encoding'
            , 'output_dirname'
            , 'timeout'
        ]
    ),
    3: namedtuple('ParamsV3', [
              'reportnet_connection'
            , 'src_attr'
            , 'src_attr_single'
            , 'src_attr_multiple'
            , 'save_file'
            , 'target_attr'
            , 'target_attr_encoding'
            , 'output_dirname'
            , 'timeout'
        ]
    )}

def create_instance(xformer_name, version, *params):
    # In Future: Use version map:s if breaking changes are introduced
    print(params)
    api_version = XFORMER_API_VERSION_MAP[version]
    params_version = XFORMER_PARAMS_VERSION_MAP[version]
    return AttachmentDownloader(xformer_name, api_version, params_version(*params))

class AttachmentDownloader(object):
    def __init__(self, xformer_name, api_version, params):
        self.xformer_name = xformer_name
        self.session = None
        
        self._logger = get_configured_logger(
              f'{self.__class__.__module__}.{self.__class__.__qualname__}'
            , fme.macroValues.get('FME_DEBUG', False)
        )
        self._log_debug(params)
        self.fmesession = fmeobjects.FMESession()
        self.params = params
        self.web_connection_name = None
        self.api_version = api_version
        self.client = None

    def _log_debug(self, msg):
        self._logger.debug('%s: %s', self.xformer_name, msg)
    def _log_info(self, msg):
        self._logger.info('%s: %s', self.xformer_name, msg)
    def _log_warn(self, msg):
        self._logger.warn('%s: %s', self.xformer_name, msg)
    def _log_error(self, msg):
        self._logger.error('%s: %s', self.xformer_name, msg)
    def _log_critical(self, msg):
        self._logger.critical('%s: %s', self.xformer_name, msg)
    
    def eval_param(self, feature, k, v):
        self._log_debug(f'evaluating parameter {k},{v}')
        if '<Unused>' == v: return None
        if type(v) == int: return v
        if 'src_attr_multiple' == k:
            # TODO: This is a major workaround for gui type ATTRLIST_ENCODED not beeing passed correctly
            # Somehow the spaces that meant to be separating individual encoded attribute names are removed
            # The workaround is to store the macro value into a temporary attribute and set parametervalue to @Value(<_temp_attribute>)
            return TransformerUtil.splitEncodedAttrs(feature.performFunction(v))
        if v.startswith('@'):
            v = feature.performFunction(v)
        return self.fmesession.decodeFromFMEParsableText(v)
    def _init_client(self, nc_name, timeout):
        credentials = rn3_fme.resolve_named_connection(nc_name)
        # fetch value using getattr (falling back to '0') because it was added in version 1
        credentials_version = getattr(credentials, 'VERSION', '0')
        if self.api_version != credentials_version:
            if not (self._params.reportnet_api_version == '2' and credentials_version == '3'):
                raise fmeobjects.FMEException('This Transformer uses version {} of the Repornet3 api but the supplied connection is of version {}.'.format(self.api_version, credentials_version))

        if self.client:
            self.client.close()
        self.client = reportnet_api.create_client(
              self.api_version
            , credentials.API_KEY
            , base_url=credentials.API_URL
            , provider_id=credentials.PROVIDER_ID
            , timeout=timeout
            , paging=None
            , log_name=f'{self.__class__.__module__}.{self.__class__.__qualname__}'
        )
        self._log_debug(f'Client created: {self.client}')
        self.web_connection_name = nc_name
    def input(self, feature):
        try:
            # when passing parameters from attributes,they need to evaluated for each feature:
            params = type(self.params)(*(self.eval_param(feature, k, v) for k,v in self.params._asdict().items()))
            self._log_debug(params)
            if not self.web_connection_name == params.reportnet_connection:
                self._init_client(params.reportnet_connection, params.timeout)
            attachment_attributes = []
            if 'Multiple' == params.src_attr:
                attachment_attributes.extend(params.src_attr_multiple)
            else:
                attachment_attributes.append(params.src_attr_single)
            for attr in attachment_attributes:
                expected_filename, dataflow_id, dataset_id, field_value_id = feature.getAttribute(attr).split(':')
                self._log_debug(f'Expecting filename `{expected_filename}`')
                if 'File' == params.save_file:
                    dir_out = os.path.join(params.output_dirname, dataflow_id, dataset_id, field_value_id)
                    if not os.path.exists(dir_out):
                        os.makedirs(dir_out)
                    fp_out = os.path.join(dir_out, expected_filename)
                    retrieved_filename = None
                    with open(fp_out, 'wb') as fout:
                        retrieved_filename = self.client.get_attachment(dataflow_id, dataset_id, field_value_id, callback=fout.write)
                    if retrieved_filename and not expected_filename == retrieved_filename:
                        rename_to = os.path.join(dir_out, retrieved_filename)
                        self._log_warn(f'Expected and retrieved filenames are not matching, renaming from `{fp_out}` to `{rename_to}`')
                        os.rename(fp_out, rename_to)
                        fp_out = rename_to
                    feature.setAttribute(attr, fp_out)
                else:
                    buf = io.BytesIO()
                    retrieved_filename = self.client.get_attachment(dataflow_id, dataset_id, field_value_id, callback=buf.write)
                    if expected_filename != retrieved_filename:
                        self._log_warn(f'Expected and retrieved filenames are not matching')
                    buf.seek(0)
                    value = buf.read()
                    if not 'fme-binary' == params.target_attr_encoding:
                        self._log_debug(f'decoding using `{params.target_attr_encoding}`')
                        value = value.decode(params.target_attr_encoding)
                    feature.setAttribute(params.target_attr, value)

            #.get_attachment(dataflow_id, dataset_id, field_value_id, callback=m.update)
        except NotImplementedError as e:
            self._log_error(str(e))
            feature.setAttribute(kFME_REJECTION_MESSAGE, str(e))
            feature.setAttribute(kFME_REJECTION_CODE, 'NOT_IMPLEMENTED')
        except Exception as e:
            self._log_error(str(e))
            feature.setAttribute(kFME_REJECTION_MESSAGE, str(e))
            feature.setAttribute(kFME_REJECTION_CODE, 'UNKNOWN')
        except fmeobjects.FMEException as e:
            feature.setAttribute(kFME_REJECTION_CODE, e.message)
        self.pyoutput(feature)
    def close(self):
        if self.client:
            self.client.close()
        del self.fmesession
if __name__ == '__main__':
    print('helo')