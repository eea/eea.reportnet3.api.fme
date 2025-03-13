"""Generic helpers for FME specific operations"""

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
    VERSION: str = '2'
    DATAFLOW_ID: str = None
    MAX_RETRIES: int = 0
    BACKOFF_FACTOR: int = 0
    RETRY_HTTP_CODES: str = ''
    RETRY_GROUP: str = ''

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
_RN3CRED_VERSIONS = {
      '0': Reportnet3CredentialsV0
    , '1': Reportnet3CredentialsV1
    , '2': Reportnet3CredentialsV2
    , '3': Reportnet3CredentialsV3
}
        

def resolve_named_connection(name: str) -> Reportnet3Credentials:
    """Resolve a named connection.

    Arguments:
    name -- Either the name of an FME Named Connection *or* a url
            in the form 
            
            * version 0-1: <api_url>?API_KEY=<token>[&PROVIDER_ID=<provider_id>][&VERSION=<version>]
            * version 2: <api_url>?VERSION=2&API_KEY=<token>&DATAFLOW_ID=<dataflow_id>[&PROVIDER_ID=<provider_id>]

            Example:
                https://test-api.reportnet.europa.eu?API_KEY=502982a2-95a5-43ae-bf3b-d16356042c86
                https://test-api.reportnet.europa.eu?VERSION=1&API_KEY=502982a2-95a5-43ae-bf3b-d16356042c86&PROVIDER_ID=5
                https://test-api.reportnet.europa.eu?VERSION=2&API_KEY=502982a2-95a5-43ae-bf3b-d16356042c86&DATAFLOW_ID=861&PROVIDER_ID=10
                https://test-api.reportnet.europa.eu?VERSION=2&API_KEY=502982a2-95a5-43ae-bf3b-d16356042c86&DATAFLOW_ID=861&PROVIDER_ID=10&MAX_RETRIES=3&backoff_factor=10000
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
