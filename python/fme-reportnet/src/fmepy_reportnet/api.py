import concurrent.futures
import itertools
import logging
import math
import re
import requests
import threading
from datetime import datetime
import time
import json
from json.decoder import JSONDecodeError
import os.path

DEFAULT_TIMEOUT = 60
TO_VALID_FILENAME = {92: '', 47: '_', 58: '', 42: '', 63: '_', 34: '', 60: '', 62: ''}
#                     \       /        :       *       ?        "       <       >

REPORTING_COUNTRIES = {
  'AL': 'Albania', 'AT': 'Austria'
, 'BA': 'Bosnia and Herzegovina', 'BE': 'Belgium', 'BG': 'Bulgaria'
, 'CH': 'Switzerland', 'CY': 'Cyprus', 'CZ': 'Czech Republic'
, 'DE': 'Germany', 'DK': 'Denmark'
, 'EE': 'Estonia', 'EL': 'Greece', 'ES': 'Spain'
, 'FI': 'Finland', 'FR': 'France'
, 'GE': 'Georgia', 'GI': 'Gibraltar'
, 'HR': 'Croatia', 'HU': 'Hungary'
, 'IE': 'Ireland', 'IS': 'Iceland', 'IT': 'Italy'
, 'LI': 'Liechtenstein', 'LT': 'Lithuania', 'LU': 'Luxembourg', 'LV': 'Latvia'
, 'MD': 'Moldova', 'ME': 'Montenegro', 'MK': 'North Macedonia', 'MT': 'Malta'
, 'NL': 'Netherlands', 'NO': 'Norway'
, 'PL': 'Poland', 'PT': 'Portugal'
, 'RO': 'Romania', 'RS': 'Serbia'
, 'SE': 'Sweden', 'SI': 'Slovenia', 'SK': 'Slovakia'
, 'TR': 'Turkey'
, 'UA': 'Ukraine', 'UK': 'United Kingdom'
, 'XK': 'Kosovo'
}

PAGING_LOGIC_OLD = 0
PAGING_LOGIC_NEW = 1
def create_client(version,*args,**kwargs):
    '''Initiates a client for communication with the API endpoint'''
    if '0' == version:
        return Reportnet3Client_v0_1(*args,**kwargs)
    elif '1' == version:
        return Reportnet3Client_v0_1(*args,**kwargs,url_version_tag='/v1')
    elif '2' == version:
        return Reportnet3Client_v0_1(*args,**kwargs,url_version_tag='/v1')
    raise Exception(f'Invalid reportnet api client version: `{version}`')

# Can override functions here if we want to change behavoiur of backoff-time.
class CustomRetry(requests.packages.urllib3.util.Retry):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def increment(self, *args, **kwargs):
        # Print a message indicating the retry
        print(f"Retrying... Attempts left: {self.total}")
        # Call the original increment method
        return super().increment(*args, **kwargs)


class Reportnet3Client_v0_1(object):
    def __init__(self, api_key, base_url='https://rn3api.eionet.europa.eu', provider_id=None, timeout=10, max_retries=0, retry_http_codes=[], backoff_factor=0, paging=None,log_name=None,url_version_tag='', debug_http_post_folder=None, paging_logic=PAGING_LOGIC_OLD):

        self.session = requests.Session()
        self.connectionErrors = 0
        # Timeout related errors not handled the same ass erroers from http codes
        if 9999 in retry_http_codes:
            self.connectionErrors = max_retries
        if max_retries:
            retry_strategy  = CustomRetry(
                  connect=self.connectionErrors
                , read=self.connectionErrors
                , backoff_factor=backoff_factor      # A delay factor for retries
                , total=max_retries                  # Total number of retries
                , status_forcelist=retry_http_codes  # HTTP status codes to retry on
                                                     # Backoff jitter not present in FME 2022 version of requests, backoff_jitter=3.0
            )
            # Mount the adapter with retries
            self.retryAdapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy )
            self.session.mount('http://', self.retryAdapter)
            self.session.mount('https://', self.retryAdapter)

        self.session.headers['Authorization'] = f'ApiKey {api_key}'
        self.api_key = api_key
        self.base_url = base_url
        if provider_id:
            self.session.params = {'providerId': provider_id}
        self.provider_id = provider_id
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_http_codes = retry_http_codes
        self.paging = paging
        self.logger = logging.getLogger(log_name or f'{self.__class__.__module__}.{self.__class__.__qualname__}')
        self.logger.debug('__init__ %s.%s %s', self.__class__.__module__, self.__class__.__qualname__, __file__)
        self.url_version_tag = url_version_tag
        self.thread_local = threading.local()
        self.debug_http_post_folder = debug_http_post_folder
        self.etl_import_batches = 0
        self.paging_logic = paging_logic
        
            
        """ TODO: Decide if we should take user_info into account
        self.user_info = self._get_user_by_user_id()
        self.provider_ids = {int(dataflow_id): provider_id for (token, dataflow_id, provider_id) in (s.split(',') for s in self.user_info.get('attributes', {}).get('ApiKeys')) if token == api_key}
        self.logger.debug('user: %s', self.user_info )
        self.logger.debug('provider_ids: %s', self.provider_ids )
        """

    def abort(self):
        pass
    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _get_user_by_user_id(self):
        url = f'{self.base_url}/user/getUserByUserId'
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        return r.json()
    def dataflows(self, timeout=None):
        '''Returns an iterator of tuples (name, id) of dataflows

        Used in order to produce a GUI that should help a user to select what dataflow to work with in FME.
        Currently no distinction is made between different kinds of dataflows (business, reference, citizenScience, ordinary?).

        Reportnet3 Endpoints
        --------------------
        Version 1:
        * POST /dataflow/businessDataflows
        * POST /dataflow/referenceDataflows
        * POST /dataflow/citizenScienceDataflows
        * POST /dataflow/getDataflows

        Version 0:
        * GET /dataflow/businessDataflows
        * GET /dataflow/referenceDataflows
        * GET /dataflow/citizenScienceDataflows
        * GET /dataflow/getDataflows

        '''
        resp = []
        urls = [
            f'{self.base_url}/dataflow/getDataflows',
            f'{self.base_url}/dataflow/businessDataflows',
            f'{self.base_url}/dataflow/citizenScienceDataflows',
            f'{self.base_url}/dataflow/referenceDataflows'
            ]
        for url in urls:
            r = self.session.post(url, timeout=timeout or self.timeout)
            r.raise_for_status()
            resp.extend(r.json()['dataflows'])
            r.close()
        # TODO: See note in __init__
        # return ((item['name'], item['id']) for item in resp if item['id'] in self.provider_ids)
        return ((f'{item["type"]} - {item["name"]}', item['id']) for item in resp)

    def dataflow_datasets(self, dataflow_id, timeout=None):
        """Returns an iterator of tuples (name, id) of datasets belonging to the specified dataflow

        Used in order to produce a GUI that should help a user to select what dataset to work with in FME.

        Reportnet3 Endpoints
        --------------------

        Version 1:
        * GET /dataflow/v1/{dataflowId}

        Version 0:
        * GET /dataflow/{dataflowId}
        """
        url = f'{self.base_url}/dataflow{self.url_version_tag}/{dataflow_id}'
        r = self.session.get(url, timeout=timeout or self.timeout)
        r.raise_for_status()
        data = r.json()


        # Diffrent methods for creating Gui labels for datasets.
        #  Prefixed by RN3 Api: dataCollections, euDatasets, referenceDatasets
        #  Prefixed by us: designDatasets
        #  Prefixed and suffixed by us: reportingDatasets

        labels = []
        for subsection in ('dataCollections', 'euDatasets', 'referenceDatasets', 'testDatasets'):
            for item in data[subsection]:
                if not self.provider_id or str(item.get('dataProviderId', None)) == self.provider_id:
                    labels.append((item['dataSetName'], item['id']))

        for item in data['reportingDatasets']:
            if not self.provider_id or str(item.get('dataProviderId', None)) == self.provider_id:
                    labels.append((f'Reporting Dataset - {item["dataSetName"]} - {item["nameDatasetSchema"]}', item['id']))

        for item in data['designDatasets']:
            if not self.provider_id or str(item.get('dataProviderId', None)) == self.provider_id:
                labels.append((f'Design Dataset - {item["dataSetName"]}', item['id']))

        return (label for label in labels)


    def table_schema_ids(self, dataflow_id, dataset_id, timeout=None):
        """Returns an iterator of table schema (name, id) tuples for the specified dataflow/dataset

        Used in conjunction with `simple_schema` in order to fully describe a table to FME.

        This is also used as a shortcut in scenarios where FME only needs a listing of table names.

        Reportnet3 Endpoints
        --------------------
        Version 1:
        GET /dataschema/v1/getTableSchemasIds/{datasetId}

        Version 0:
        GET /dataschema/getTableSchemasIds/{datasetId}
        """
        url = f'{self.base_url}/dataschema{self.url_version_tag}/getTableSchemasIds/{dataset_id}'
        params = {'dataflowId': dataflow_id}
        r = self.session.get(
            url,
            params=params,
            timeout=timeout or self.timeout
        )
        r.raise_for_status()
        return ((item['nameTableSchema'], item['idTableSchema']) for item in r.json())

    def simple_schema(self, dataflow_id, dataset_id, timeout=None):
        """Returns an iterator of table definitions for the specified dataflow/dataset

        Used in order to describe to FME the details (field name/type etc.) of each table

        Reportnet3 Endpoints
        --------------------

        Version 1:
        * GET /dataschema/v1/getSimpleSchema/dataset/{datasetId}

        Version 0:
        * GET /dataschema/getSimpleSchema/dataset/{datasetId}

        """
        url = f'{self.base_url}/dataschema{self.url_version_tag}/getSimpleSchema/dataset/{dataset_id}'
        params = {'dataflowId': dataflow_id}
        r = self.session.get(
            url,
            params=params,
            timeout=timeout or self.timeout
        )
        r.raise_for_status()
        return (t for t in r.json()['tables'])

    def delete_import_table(self, dataflow_id, dataset_id, table_schema_id, timeout=60):
        url = f'{self.base_url}/dataset/{dataset_id}/deleteImportTable/{table_schema_id}'
        params = {"dataflowId": dataflow_id}
        r = self.session.delete(url, params=params, timeout=timeout or self.timeout)
        r.raise_for_status()
        #https://rn3api.eionet.europa.eu/dataset/6898/deleteImportTable/612f36c6cd0fd400016cf9b5?dataflowId={{dataflowId}}

    def etl_import(self, dataflow_id, dataset_id, data, timeout=None):
        """Write table records to Reportnet3

        Reportnet3 Endpoints
        --------------------

        Version 1:
        * POST /dataset/v1/{datasetId}/etlImport

        Version 0:
        * POST /dataset/{datasetId}/etlImport
        """
        ts = datetime.now()
        tic = time.perf_counter()
        self.etl_import_batches += 1
        url = f'{self.base_url}/dataset{self.url_version_tag}/{dataset_id}/etlImport'
        params = {"dataflowId": dataflow_id}
        if self.provider_id:
            params['providerId'] = self.provider_id
        r = self.session.post(url, params=params, json=data, timeout=timeout or self.timeout or DEFAULT_TIMEOUT)
        if self.debug_http_post_folder:
            toc = time.perf_counter()
            basename = '_'.join([
                  ts.strftime('%Y%m%d_%H%M%S')
                , f'{toc - tic:.2f}'
                , str(self.etl_import_batches)
                , url
                , '&'.join(f'{k}={v}' for k,v in params.items())
                , str(r.status_code)
                ]).translate(TO_VALID_FILENAME)
            fp = os.path.join(self.debug_http_post_folder, f'{basename}.json')
            with open(fp, 'w', encoding='utf8') as fout:
                json.dump(data, fout, ensure_ascii=False)
            
        r.raise_for_status()
        r.close()

    def etl_export(self, dataflow_id, dataset_id, table_schema_id, limit=None, timeout=None, concurrent_http_requests=None, filter=None, data_provider_codes=None, ordered=False):
        """Returns an iterator of table records

        Used in FME to read data from Reportnet3

        Reportnet3 Endpoints
        --------------------
        Version 1 (skipping ahead...):
        * GET /dataset/v2/etlExport/{datasetId}

        Version 0:
        * GET /dataset/{datasetId}/etlExport
        """
        if filter:
            if not dict == type(filter):
                raise Exception('filter should be a dictionary with exactly two keys: columnName and filterValue')
        url = f'{self.base_url}/dataset/v2/etlExport/{dataset_id}'
        page_size = self.paging or 15000
        if limit is not None:
            page_size = min(limit, page_size)
        params = {
            'dataflowId': dataflow_id,
            'limit': page_size,
            'tableSchemaId': table_schema_id
        }
        if filter is not None:
            params.update(filter)
        if data_provider_codes is not None:
            params['dataProviderCodes'] = data_provider_codes
        if self.provider_id is not None and len(self.provider_id):
            params['providerId'] = self.provider_id
            
        # We need to communicate across threads that an empty page was
        # encountered
        empty_page_encountered = threading.Event()
        def get_page(page_nbr):
            if not hasattr(self.thread_local, 'session'):
                # creating a private session for this thread
                self.thread_local.session = requests.Session()
                if self.max_retries:         
                    self.thread_local.session.mount('http://', self.retryAdapter)
                    self.thread_local.session.mount('https://', self.retryAdapter)
                self.thread_local.session.headers['Authorization'] = f'ApiKey {self.api_key}'
            
            if empty_page_encountered.is_set():
                # Normally this should not happen but if the endpoint reports wrong nbr of total records we may end up here.
                self.logger.error('Aborting due to empty page')
                return 0, page_nbr, []
            page_params = {**params}
            if PAGING_LOGIC_OLD == self.paging_logic:
                # When using the old paging logic, page 0 (zero) is 
                # used only to fetch total nbr of records
                if 0 == page_nbr:
                    page_params['limit'] = 0
                page_params['offset'] = page_nbr
            elif PAGING_LOGIC_NEW == self.paging_logic:
                page_params['offset'] = page_nbr * page_size
            self.logger.debug('Requesting url %s using params %s', url, page_params)
            r = self.thread_local.session.get(
                url,
                params=page_params,
                timeout=timeout or self.timeout
            )
            r.raise_for_status()
            data = dict()
            try:
                data = r.json()
            except JSONDecodeError as decodeError:
                '''
                |  Subclass of ValueError with the following additional properties:
                |
                |  msg: The unformatted error message
                |  doc: The JSON document being parsed
                |  pos: The start index of doc where parsing failed
                |  lineno: The line corresponding to pos
                |  colno: The column corresponding to pos 
                '''
                self.logger.error('Error while fetching page %s %s. Error message was: `%s`', url, params, decodeError.msg)
                self.logger.error('json data was: `%s`', decodeError.doc)
                raise decodeError
            # The response model is different when we retrieve an empty result:
            # Empty:
            #  {'tables': [{'totalRecords': 0, 'records': [], 'tableName': 'BIOREGION'}]}
            # Some:
            # {'tableName': 'BIOREGION', 'totalRecords': 1, 'records': [{'countryCode': None, 'fields': [{'fieldName': 'SITECODE', 'value': 'a', 'field_value_id': None}, ...]}]}
            if list == type(data.get('tables', None)):
                # Expecting exactly one item in tables list
                # self.logger.debug('%s', data)
                data = data['tables'][0]
            total_records = int(data.get('totalRecords', 0))
            if not total_records:
                self.logger.warn('totalRecords %s', total_records)
                return 0, page_nbr, []
            records = data.get('records', [])
            if not len(records) and (PAGING_LOGIC_NEW == self.paging_logic or page_nbr > 0):
                # no records, this could happen if the initial calculation of total nbr pages was wrong
                empty_page_encountered.set()
                self.logger.warning('page_nbr %s was empty', page_nbr)
                return 0, page_nbr, []
            self.logger.debug('page_nbr %s, %s records', page_nbr, len(records))
            return total_records,page_nbr,records
        total_records,_,page_zero = get_page(0)
        records_left_to_fetch = total_records - len(page_zero)
        if limit is not None:
            records_left_to_fetch = min(limit - len(page_zero), records_left_to_fetch)
        pages_left_to_fetch = math.ceil(records_left_to_fetch / page_size)
        additional_page_nbrs = range(1, pages_left_to_fetch + 1, 1)
        self.logger.debug('page zero contains %s records', len(page_zero))
        self.logger.debug('total_records: %s, fetching another %s record(s) in %s page(s)', total_records, records_left_to_fetch, len(additional_page_nbrs))
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_http_requests) as executor:
            additional_pages = []
            if ordered:
                additional_pages = executor.map(get_page, additional_page_nbrs)
            else:
                additional_pages = (
                    future.result()
                    for future in concurrent.futures.as_completed(
                        (
                            executor.submit(get_page, page_nbr)
                            for page_nbr in additional_page_nbrs
                        )
                    )
                )
            pages = itertools.chain([(total_records,0,page_zero)], additional_pages)
            records = ((page_nbr, rec) for _, page_nbr,page in pages for rec in page)
            for i, (_, rec) in enumerate(records):
                if limit and i >= limit:
                    break
                yield rec

    def get_attachment(self, dataflow_id, dataset_id, field_value_id, callback=lambda chunk: print(len(chunk)), timeout=None):
        """Download an attachment (document) from Reportnet3

        Parameters
        ----------
        callback: a function that will accept chunks of data, called repeateadly until download is complete.
        returns: the filename or None if it could not be parsed from Content-Disposition header

        Example usage::

           with open('myfile.txt', 'wb') as fout:
               filename = client.get_attachment('123','456','ASDF25345', fout.write, 60) or 'unknown-remote-file-name'
               print('downloaded', filename, 'to local file myfile.txt')

        Reportnet3 Endpoints
        --------------------

        Version 1:
        * GET /dataset/v1/{datasetId}/field/{fieldId}/attachment

        Version 0:
        * GET /dataset/{datasetId}/field/{fieldId}/attachment
        """
        url = f'{self.base_url}/dataset{self.url_version_tag}/{dataset_id}/field/{field_value_id}/attachment'
        self.logger.debug('downloading attachment using url `%s`', url)
        params={"dataflowId": dataflow_id}
        with self.session.get(
            url,
            params=params,
            timeout=timeout or self.timeout,
            stream=True
        ) as r:
            r.raise_for_status()
            #attachment; filename=thepoints(2).csv
            filename = None
            m = re.match(r'attachment; filename=(.*)$', r.headers.get('Content-Disposition', ''))
            if m:
                filename = m.group(1)
            chunksize = [8192, None]['chunked' == r.headers.get('Transfer-Encoding')]
            for chunk in r.iter_content(chunk_size=chunksize):
                callback(chunk)
            return filename

    def _selftest(self,*args):
        for dataflow_name,dataflow_id in self.dataflows():
            print(dataflow_name,dataflow_id)
            try:
                for dataset_name, dataset_id in self.dataflow_datasets(dataflow_id):
                    print(' ', dataset_name, dataset_id)
                    try:
                        for table_name, table_id in self.table_schema_ids(dataflow_id, dataset_id):
                            print('  ', table_name, table_id)
                    except Exception as e:
                        print(' ', e)

                    if not '-v' in args: continue
                    print('   ', 'get more info here...')
                    try:
                        for t in self.simple_schema(dataflow_id, dataset_id):
                            print('  ', t)
                    except Exception as e:
                        print(' ', e)

            except Exception as e:
                print(' ', e)
    def __str__(self):
        properties = {s: getattr(self, s, '') for s in [
              'connectionErrors'
            , 'api_key'
            , 'base_url'
            , 'provider_id'
            , 'timeout'
            , 'max_retries'
            , 'backoff_factor'
            , 'retry_http_codes'
            , 'paging'
            , 'url_version_tag'
            , 'debug_http_post_folder'
            , 'etl_import_batches']
        }
        return f'{type(self).__name__}({properties})'

    # ceil without using math module
    def ceil(self, n):
        res = int(n)
        return res if res == n or n < 0 else res+1

if __name__ == '__main__':
    import sys
    client_version = '0'
    api_key,*rem = sys.argv[1:]
    client = create_client(client_version, api_key)
    client._selftest(*rem)
