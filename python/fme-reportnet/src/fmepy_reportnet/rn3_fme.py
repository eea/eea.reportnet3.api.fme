"""Generic helpers for FME specific operations"""

VERSION_COMPATIBILITY_RN3API_WEBSVC = [
    # We don't want to run a new workspace using an old named connection
    # rn3-api, web-service
     ('1', '1')
    ,('1', '2')
    ,('2', '2')
    ,('1', '3')
    ,('2', '3')
]

from dataclasses import dataclass
from abc import ABC
class Reportnet3Credentials(ABC):
    pass
@dataclass
class _RN3CredV0Mixin:
    API_URL: str
    API_KEY: str
    PROVIDER_ID: str = None
@dataclass
class _RN3CredV1Mixin:
    VERSION: str = '1'
@dataclass
class _RN3CredV2Mixin:
    VERSION: str = '2'
    DATAFLOW_ID: str = None
@dataclass
class _RN3CredV3Mixin:
    VERSION: str = '3'
    DATAFLOW_ID: str = None
    MAX_RETRIES: int = 0
    BACKOFF_FACTOR: int = 0
    RETRY_HTTP_CODES: str = ''
    RETRY_GROUP: str = ''
@dataclass
class _RN3CredV4Mixin:
    VERSION: str = '4'
    DATAFLOW_ID: str = None
    MAX_RETRIES: int = 0
    BACKOFF_FACTOR: int = 0
    RETRY_HTTP_CODES: str = ''
    RETRY_GROUP: str = ''
    RETRY_GROUP: str = ''
    PAGING_LOGIC: str = 'OLD'

@dataclass
class Reportnet3CredentialsV0(Reportnet3Credentials,_RN3CredV0Mixin):
    pass
@dataclass
class Reportnet3CredentialsV1(Reportnet3Credentials,_RN3CredV1Mixin,_RN3CredV0Mixin):
    pass
@dataclass
class Reportnet3CredentialsV2(Reportnet3Credentials,_RN3CredV2Mixin,_RN3CredV0Mixin):
    pass
@dataclass
class Reportnet3CredentialsV3(Reportnet3Credentials,_RN3CredV3Mixin,_RN3CredV0Mixin):
    pass
@dataclass
class Reportnet3CredentialsV4(Reportnet3Credentials,_RN3CredV4Mixin,_RN3CredV0Mixin):
    pass
_RN3CRED_VERSIONS = {
      '0': Reportnet3CredentialsV0
    , '1': Reportnet3CredentialsV1
    , '2': Reportnet3CredentialsV2
    , '3': Reportnet3CredentialsV3
    , '4': Reportnet3CredentialsV4
}
        

def resolve_named_connection(name: str) -> Reportnet3Credentials:
    """Resolve a named connection.

    Arguments:
    name -- Either the name of an FME Named Connection *or* a url
            in the form 
            
            * version 0-1: <api_url>?API_KEY=<token>[&PROVIDER_ID=<provider_id>][&VERSION=<version>]
            * version 2: <api_url>?VERSION=2&API_KEY=<token>&DATAFLOW_ID=<dataflow_id>[&PROVIDER_ID=<provider_id>]
            * version 3: <api_url>?VERSION=3&API_KEY=<token>&DATAFLOW_ID=<dataflow_id>[&PROVIDER_ID=<provider_id>][&MAX_RETRIES=<max_retries>][&BACKOFF_FACTOR=<backoff_factor>][&RETRY_HTTP_CODES=<retry_http_codes>]
            * version 4: <api_url>?VERSION=3&API_KEY=<token>&DATAFLOW_ID=<dataflow_id>[&PROVIDER_ID=<provider_id>][&MAX_RETRIES=<max_retries>][&BACKOFF_FACTOR=<backoff_factor>][&RETRY_HTTP_CODES=<retry_http_codes>][&PAGING_LOGIC=OLD|NEW]

            Example:
                https://test-api.reportnet.europa.eu?API_KEY=502982a2-95a5-43ae-bf3b-d16356042c86
                https://test-api.reportnet.europa.eu?VERSION=1&API_KEY=502982a2-95a5-43ae-bf3b-d16356042c86&PROVIDER_ID=5
                https://test-api.reportnet.europa.eu?VERSION=2&API_KEY=502982a2-95a5-43ae-bf3b-d16356042c86&DATAFLOW_ID=861&PROVIDER_ID=10
                https://test-api.reportnet.europa.eu?VERSION=3&API_KEY=502982a2-95a5-43ae-bf3b-d16356042c86&DATAFLOW_ID=861&PROVIDER_ID=10&MAX_RETRIES=3&BACKOFF_FACTOR=10&BACKOFF_FACTOR&RETRY_HTTP_CODES=401,403
                https://test-api.reportnet.europa.eu?VERSION=4&API_KEY=502982a2-95a5-43ae-bf3b-d16356042c86&DATAFLOW_ID=861&PROVIDER_ID=10&MAX_RETRIES=3&BACKOFF_FACTOR=10&BACKOFF_FACTOR&RETRY_HTTP_CODES=401,403&PAGING_LOGIC=NEW
    """
    from fmeobjects import FMEException
    from fmewebservices import FMENamedConnectionManager
    nc = FMENamedConnectionManager().getNamedConnection(name)
    if nc:
        import re
        m = re.match('eea.reportnet.Reportnet 3( v(\d+))?', nc.getWebService().getName())
        if not m:
            raise FMEException(f'`{name}` is not a valid Reportnet3 Connection')
        version = m.group(2) or '0'
        if not version in _RN3CRED_VERSIONS:
            raise FMEException(f'Unrecognized  Reportnet3 Connection version `{version}`')
        kvp = {**nc.getKeyValues()}
        kvp['API_URL'] = kvp['API_URL'].rstrip('/')
        return _RN3CRED_VERSIONS[version](**kvp)
    from urllib.parse import urlparse, parse_qsl
    o = urlparse(name)
    if not (o.scheme in ['http', 'https'] and o.netloc):
        raise FMEException(f'Invalid Connection: `{name}`')
    api_url = f'{o.scheme}://{o.netloc}/{o.path}'.rstrip('/')
    try:
        params = dict(parse_qsl(o.query))
        version = params.get('VERSION', '0')
        if not version in _RN3CRED_VERSIONS:
            raise FMEException(f'Unrecognized  Reportnet3 Connection version `{version}`')
        return _RN3CRED_VERSIONS[version](**{'API_URL': api_url, **params})
    except Exception as e:
        raise FMEException(f'Could not parse connection string `{name}`. {e}')
