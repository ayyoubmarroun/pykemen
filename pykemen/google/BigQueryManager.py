"""BigQuery Manager module.

This module have a class with all needed functionalites
of BigQuery that AudienceScore needs.
"""
__author__ = 'Metriplica-Ayyoub&Javier'

import time
import pykemen.utilities as utilities
from googleapiclient.discovery import build


class BigQuery(object):
    """BigQuery class.

    Manage bigQuery tables and properties.
    """

    def __init__(self, secrets, credentials):
        """Init module initialize and create BigQuery class.

        Args:
            secrets (str): Secrets of the Google accout to use
            credentials (str): Credentials to access to the client services

        Returns:
            BigQuery: with given configuration.
        """
        scopes = ["https://www.googleapis.com/auth/gmail.send", "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/bigquery.insertdata"]
        self._bigqueryService = utilities.create_api('bigquery', 'v2', scopes, secrets, credentials)

    def createTable(self, projectId, datasetId, tableId, query, useLegacySql=True):
        """Create table function.

        Create a table in the projectId/dataset at bigQuery.
        If the table alredy exists its gona be overwrited.

        Args:
            tableId   (str): Name of the table to createTable.
            query       (str): Query to store as a table.

        Returns:
            bool: True for success, Raises an error otherwise.

        """
        query_data = {
            'configuration': {
                'query': {
                    'query': query,
                    'useLegacySql': useLegacySql,
                    'allowLargeResults': True,
                    'destinationTable': {
                        'projectId': projectId,
                        'tableId': tableId,
                        'datasetId': datasetId
                    },
                    'createDisposition': 'CREATE_IF_NEEDED',
                    'writeDisposition': 'WRITE_IF_EMPTY'
                }
            }
        }
        job = self._bigqueryService.jobs().insert(
            projectId=projectId, body=query_data).execute()
        jobId = job['jobReference']['jobId']
        job = self._bigqueryService.jobs().get(
            projectId=projectId, jobId=jobId).execute()
        while job['status']['state'] == 'RUNNING':  # OR RUNNING?
            time.sleep(10)
            job = self._bigqueryService.jobs().get(
                projectId=projectId, jobId=jobId).execute()
        if job.get('status').get('state') == 'DONE' and not job.get('status').get('errors') and not job.get('status').get('errorResult'):  # Job done successfully
            return True
        raise Exception('BigQuery table creation', job)

    def overwriteTable(self, projectId, datasetId, tableId, query, useLegacySql=True):
        """Create table function.

        Create a table in the projectId/dataset at bigQuery.
        If the table alredy exists its gona be overwrited.

        Args:
            tableId   (str): Name of the table to createTable.
            query       (str): Query to store as a table.

        Returns:
            bool: True for success, Raises an error otherwise.

        """
        query_data = {
            'configuration': {
                'query': {
                    'query': query,
                    'useLegacySql': useLegacySql,
                    'allowLargeResults': True,
                    'destinationTable': {
                        'projectId': projectId,
                        'tableId': tableId,
                        'datasetId': datasetId
                    },
                    'createDisposition': 'CREATE_NEVER',
                    'writeDisposition': 'WRITE_TRUNCATE'
                }
            }
        }
        job = self._bigqueryService.jobs().insert(
            projectId=projectId, body=query_data).execute()
        jobId = job['jobReference']['jobId']
        job = self._bigqueryService.jobs().get(
            projectId=projectId, jobId=jobId).execute()
        while job['status']['state'] == 'RUNNING':
            time.sleep(60)
            job = self._bigqueryService.jobs().get(
                projectId=projectId, jobId=jobId).execute()
        if job.get('status').get('state') == 'DONE' and not job.get('status').get('errors') and not job.get('status').get('errorResult'):  # Job done successfully
            return True
        raise Exception('BigQuery overwrite table', job)

    def appendTable(self, projectId, datasetId, tableId, query, useLegacySql=True):
        """Append to a specified table the result of the specified query.

        Args:
            tableId   (str): Name of the table to append data.
            query       (str): Query to append to the table.

        Returns:
            bool: True for success, Raises an error otherwise.

        """
        query_data = {
            'configuration': {
                'query': {
                    'query': query,
                    'allowLargeResults': True,
                    'useLegacySql': useLegacySql,
                    'destinationTable': {
                        'projectId': projectId,
                        'tableId': tableId,
                        'datasetId': datasetId
                    },
                    'createDisposition': 'CREATE_NEVER',
                    'writeDisposition': 'WRITE_APPEND'
                }
            }
        }
        job = self._bigqueryService.jobs().insert(
            projectId=projectId, body=query_data).execute()
        jobId = job['jobReference']['jobId']
        job = self._bigqueryService.jobs().get(
            projectId=projectId, jobId=jobId).execute()
        while job['status']['state'] == 'RUNNING':
            time.sleep(60)
            job = self._bigqueryService.jobs().get(
                projectId=projectId, jobId=jobId).execute()
        if job.get('status').get('state') == 'DONE' and not job.get('status').get('errors') and not job.get('status').get('errorResult'):  # Job done successfully
            return True
        raise Exception('BigQuery append table', job)

    def deleteTable(self, projectId, datasetId, tableId):
        """Delete table function.

        Delete a table from the projectId/dataset at bigQuery.

        Args:
            tableId   (str): Name of the table to createTable.
            query       (str): Query to store as a table.

        Returns:
            bool: True for success, Raises an error otherwise.

        """
        job = self._bigqueryService.tables().delete(
            projectId=projectId,
            datasetId=datasetId,
            tableId=tableId
        ).execute()  # response = ''
        if job == "":
            return True
        # jobId = job['jobReference']['jobId']
        # while job['status']['state'] == 'PENDING':
        #     time.sleep(60)
        #     job = self._bigqueryService.jobs().get(
        #         projectId=projectId, jobId=jobId).execute()
        # if job['status']['state'] == 'DONE':  # Job done successfully
        #     return True
        raise Exception('BigQuery delete table', job)

    def saveQuerytoCSV(self, filename, projectId, query, header=None, delimiter=','):
        """Save the result of a query into a CSV.

        The CSV header are formed by de custom dimensions
        that are in the configuration of AS.

        Args:
            filename(str):  Name of the file to save.
            query   (str): Name of the query to request to BigQuery.

        Returns:
            bool: True if the query have results. False otherwise.

        """
        queryResponse = self._bigqueryService.jobs().query(
            projectId=projectId,
            body={'query': query}).execute()
        # Wait until the query is done
        while not queryResponse['jobComplete']:
            time.sleep(60)
            queryResponse = self._bigqueryService.jobs().getQueryResults(
                projectId=projectId,
                jobId=queryResponse['jobReference']['jobId']).execute()
        if int(queryResponse['totalRows']) <= 0:
            return False
        CSVString = self._queryJsonToCSV(projectId, queryResponse, header, delimiter)
        with open(filename, 'w') as f:
            f.write(CSVString)
        return True

    def _queryJsonToCSV(self, projectId, queryResponse, header=None, delimiter=','):
        """Give a CSV string from a query response JSON.

        Transforms the response table in JSON format to
        a CSV string and returne it back.

        Args:
            queryResponse(dict):    Query response of bigQuery.

        Returns:
            str: A csv string.

        """
        CSVContet = ""
        if not header:
            header = []
            for i in queryResponse['schema']['fields']:
                header.append(utilities.xstr(i['name']))
        CSVContet += delimiter.join(header)
        # CSVContet = "ga:dimension%s,gadimension%s" % (
        #    self.config['cd1'], self.config['cd2'])
        for row in queryResponse['rows']:
            line = []
            for column in row['f']:
                line.append(utilities.xstr(column['v']))
            CSVContet += '\n' + ','.join(line)
        while 'pageToken' in queryResponse.keys() and queryResponse[
                'pageToken'] is not None:
            queryResponse = self._bigqueryService.jobs().getQueryResults(
                projectId=projectId,
                jobId=queryResponse['jobReference']['jobId'],
                pageToken=queryResponse['pageToken']).execute()
            for row in queryResponse['rows']:
                line = []
                for column in row['f']:
                    line.append(utilities.xstr(column['v']))
                CSVContet += '\n' + ','.join(line)
        return CSVContet

    def isTableCreated(self, projectId, datasetId, tableId):
        """Check if the specified table is created.

        Args:
            tableId   (str):  Table name to Check.

        Returns:
            bool: True if exists, false otherwhise.

        """
        try:
            self._bigqueryService.tables().get(
                projectId=projectId,
                datasetId=datasetId,
                tableId=tableId).execute()
            return True
        except Exception:  # noqa
            return False

    def getTableProperties(self, projectId, datasetId, tableId, fields):
        """Get properties of a bigQuery table.

        Args:
            projectId (str): BigQuery project.
            datasetId (str): Dataset id of the table.
            tableId (str): Table id.
            fields (str): Requested properties

        Returs:
            dict: Json with the specified properties.

        """
        return self._bigqueryService.tables().get(
            projectId=projectId,
            datasetId=datasetId,
            tableId=tableId,
            fields=fields
        ).execute()

    def createDataset(self, projectId, datasetId):
        """Create a dataset in the specified project.

        Args:
            projectId (str): bigQuery projectId.
            datasetId (str): datasetId to create.

        Returns:
            bool: True if succesfully created. Raises error otherwhise.
        """
        body = {"datasetReference": {"datasetId": datasetId}}
        response = self._bigqueryService.datasets().insert(
            projectId=projectId,
            body=body).execute()
        if response.get('kind') == 'bigquery#dataset':
            return True
        raise Exception('BigQuery create dataset', response)
