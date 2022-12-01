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
import os.path
DEFAULT_TIMEOUT = 60
TO_VALID_FILENAME = {92: '', 47: '_', 58: '', 42: '', 63: '_', 34: '', 60: '', 62: ''}
#                     \       /        :       *       ?        "       <       >


def create_client(version,*args,**kwargs):
    '''Initiates a client for communication with the API endpoint'''
    if '0' == version:
        return Reportnet3Client_v0_1(*args,**kwargs)
    elif '1' == version:
        return Reportnet3Client_v0_1(*args,**kwargs,url_version_tag='/v1')
    elif '2' == version:
        return Reportnet3Client_v0_1(*args,**kwargs,url_version_tag='/v1')
    raise Exception(f'Invalid reportnet api client version: `{version}`')


class Reportnet3Client_v0_1(object):
    def __init__(self, api_key, base_url='https://rn3api.eionet.europa.eu', provider_id=None, timeout=10, paging=None,log_name=None,url_version_tag='', debug_http_post_folder=None):

        self.session = requests.Session()
        self.session.headers['Authorization'] = f'ApiKey {api_key}'
        self.api_key = api_key
        self.base_url = base_url
        if provider_id:
            self.session.params = {'providerId': provider_id}
        self.provider_id = provider_id
        self.timeout = timeout
        self.paging = paging
        self.logger = logging.getLogger(log_name or f'{self.__class__.__module__}.{self.__class__.__qualname__}')
        self.logger.debug('__init__ %s.%s %s', self.__class__.__module__, self.__class__.__qualname__, __file__)
        self.url_version_tag = url_version_tag
        self.thread_local = threading.local()
        self.debug_http_post_folder = debug_http_post_folder
        self.etl_import_batches = 0
        
            
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
        def get_page(page_nbr):
            if not hasattr(self.thread_local, 'session'):
                # creating a private session for this thread
                self.thread_local.session = requests.Session()
                self.thread_local.session.headers['Authorization'] = f'ApiKey {self.api_key}'
            if not hasattr(self.thread_local, 'empty_page_at'):
                # We keep track of if empty page is encountered in each thread so that we can abort early in case total_records is misleading
                self.thread_local.empty_page_at = 0
            if self.thread_local.empty_page_at and page_nbr > self.thread_local.empty_page_at:
                # Normally this should not happen but if the endpoint reports wrong nbr of total records we may end up here.
                #print(f'thread {threading.get_ident()}, page_nbr {page_nbr} - aborting due to empty page at {self.thread_local.empty_page_at}')
                return 0, page_nbr, []

            r = self.thread_local.session.get(
                url,
                params={**params, **({ "limit": 0 } if not page_nbr else {}), "offset": page_nbr},
                timeout=timeout or self.timeout
            )
            r.raise_for_status()
            data = r.json()
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
            if not len(records) and page_nbr > 0:
                # print(f'thread {threading.get_ident()}, page_nbr {page_nbr} - no records ({records})')
                # no records, this could happen if the initial calculation of total nbr pages was wrong
                self.thread_local.empty_page_at = page_nbr
                #raise Exception('no records')
                return 0, page_nbr, []
            self.logger.debug('page_nbr %s, %s records', page_nbr, len(records))
            return total_records,page_nbr,records
        total_records,_,page_one = get_page(0)
        records_to_fetch = total_records
        if limit is not None:
            records_to_fetch = min(limit, total_records)
        last_page_nbr = math.ceil(records_to_fetch / page_size)
        additional_page_nbrs = range(1, last_page_nbr + 1, 1)
        self.logger.debug('total_records: %s, fetching %s records in %s pages', total_records, records_to_fetch, len(additional_page_nbrs))
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
            pages = itertools.chain([(total_records,1,page_one)], additional_pages)
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
        return f'{type(self).__name__}(base_url: {self.base_url}, paging: {self.paging})'

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
