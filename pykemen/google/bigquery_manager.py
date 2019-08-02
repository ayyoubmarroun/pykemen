"""BigQuery Manager module.

This module have a class with all needed functionalites
of BigQuery that AudienceScore needs.
"""
__author__ = 'Metriplica-Ayyoub&Javier'

import time
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

class BigQuery(object):
    """BigQuery class.

    Manage bigQuery tables and properties.
    """

    def __init__(self, project=None, location="US"):
        """Init module initialize and create BigQuery class.

        Args:
            credentials (dcit): Credentials to access to the client services
            secrets (dict): Secrets of the Google accout to use

        Returns:
            BigQuery: with given configuration.
        """
        self.bigquery_client = bigquery.Client(project=project, location=location)

    def create_table(self, project_id, dataset_id, table_id, query, legacy=True):
        """Create table function.

        Create a table in the project_id/dataset at bigQuery.
        If the table alredy exists its gona be overwrited.

        Args:
            project_id (str): BigQuery project id
            table_id   (str): Name of the table to createTable.
            query       (str): Query to store as a table.

        Returns:
            bool: True for success, Raises an error otherwise.

        """
        table = self.bigquery_client.dataset(dataset_id, project_id).table(table_id)
        query_job_config = bigquery.QueryJobConfig()
        query_job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
        query_job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        query_job_config.destination = table
        query_job_config.use_legacy_sql = legacy
        query_job_config.allow_large_results = True
        query_job = self.bigquery_client.query(query, query_job_config)
        query_job.result()
        while query_job.running():
            time.sleep(60)
        if query_job.done():
            return True
        elif query_job.cancelled():
            raise Exception("BigQuery table creation", query_job.errors)

    def create_empty_table(self, project_id, dataset_id, table_id, schema, partition_field=None, expiration=None):
        """Create table function.

        Create a table in the project_id/dataset at bigQuery.
        If the table alredy exists its gona be overwrited.

        Args:
            project_id (str): BigQuery project id
            table_id   (str): Name of the table to createTable.
            query      (str): Query to store as a table.

        Returns:
            bool: True for success, Raises an error otherwise.

        """

        table_ref = self.bigquery_client.dataset(dataset_id, project_id).table(table_id)
        if isinstance(schema, list):
            schema = self._get_schema_from_json(schema)
        elif isinstance(schema, str):
            schema = self._get_schema_from_str(schema)
        table = bigquery.Table(table_ref, schema=schema)
        if partition_field: 
            expiration_ms = expiration*24*60*60*1000 if expiration else None
            partitioning = bigquery.TimePartitioning(field=partition_field, expiration_ms=expiration_ms)
            table.time_partitioning = partitioning
        table_ref = self.bigquery_client.create_table(table)

    @staticmethod
    def _get_schema_from_str(schema_str):
        schema = []
        for field in schema_str.split(","):
            name, type_ = field.split(":")
            schema.append(bigquery.SchemaField(name, type_))
        return schema
    
    @staticmethod
    def _get_schema_from_json(schema):
        for field in schema:
            field = bigquery.SchemaField.from_api_repr(field)
        return schema
       
    def overwrite_table(self, project_id, dataset_id, table_id, query, legacy=True):
        """Create table function.

        Create a table in the projectId/dataset at bigQuery.
        If the table alredy exists its gona be overwrited.

        Args:
            table_id   (str): Name of the table to createTable.
            query       (str): Query to store as a table.

        Returns:
            bool: True for success, Raises an error otherwise.

        """
        table = self.bigquery_client.dataset(dataset_id, project_id).table(table_id)
        query_job_config = bigquery.QueryJobConfig()
        query_job_config.create_disposition = bigquery.CreateDisposition.CREATE_NEVER
        query_job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        query_job_config.destination = table
        query_job_config.use_legacy_sql = legacy
        query_job_config.allow_large_results = True
        query_job = self.bigquery_client.query(query, query_job_config)
        query_job.result()
        while query_job.running():
            time.sleep(60)
        if query_job.done():
            return True
        elif query_job.cancelled():
            raise Exception("BigQuery table creation", query_job.errors)

    def append_table(self, project_id, dataset_id, table_id, query, legacy=True):
        """Append to a specified table the result of the specified query.

        Args:
            tableId   (str): Name of the table to append data.
            query       (str): Query to append to the table.

        Returns:
            bool: True for success, Raises an error otherwise.

        """
        table = self.bigquery_client.dataset(dataset_id, project_id).table(table_id)
        query_job_config = bigquery.QueryJobConfig()
        query_job_config.create_disposition = bigquery.CreateDisposition.CREATE_NEVER
        query_job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        query_job_config.destination = table
        query_job_config.use_legacy_sql = legacy
        query_job_config.allow_large_results = True
        query_job = self.bigquery_client.query(query, query_job_config)
        query_job.result()
        while query_job.running():
            time.sleep(60)
        if query_job.done():
            return True
        elif query_job.cancelled():
            raise Exception("BigQuery table creation", query_job.errors)

    def delete_table(self, project_id, dataset_id, table_id):
        """Delete table function.

        Delete a table from the projectId/dataset at bigQuery.

        Args:
            tableId   (str): Name of the table to createTable.
            query       (str): Query to store as a table.

        Returns:
            bool: True for success, Raises an error otherwise.

        """

        table = self.bigquery_client.dataset(dataset_id).table(table_id)
        self.bigquery_client.delete_table(table)
        return True

    def save_query2csv(self, filename, project_id, query, header=None, delimiter=',', legacy=True):
        """Save the result of a query into a CSV.

        The CSV header are formed by de custom dimensions
        that are in the configuration of AS.

        Args:
            filename(str):  Name of the file to save.
            query   (str): Name of the query to request to BigQuery.

        Returns:
            bool: True if the query have results. False otherwise.

        """
        query_job_config = bigquery.QueryJobConfig()
        query_job_config.use_legacy_sql = legacy
        query_job = self.bigquery_client.query(query,project=project_id, job_config=query_job_config)
        query_job.result()
        while query_job.running():
            time.sleep(60)
        if query_job.done():
            data = query_job.to_dataframe()
            if header:
                data.columns = header
            data.to_csv(filename, sep=delimiter, index=False)
            return True
        elif query_job.cancelled():
            raise Exception("BigQuery table creation", query_job.errors)

    def is_table_created(self, project_id, dataset_id, table_id):
        """Check if the specified table is created.

        Args:
            tableId   (str):  Table name to Check.

        Returns:
            bool: True if exists, false otherwhise.

        """
        try:
            table_ref = self.bigquery_client.dataset(dataset_id, project_id).table(table_id)
            table = self.bigquery_client.get_table(table_ref)
            return True
        except NotFound:  # noqa
            return False

    def get_table_properties(self, project_id, dataset_id, table_id):
        """Get properties of a bigQuery table.

        Args:
            projectId (str): BigQuery project.
            datasetId (str): Dataset id of the table.
            tableId (str): Table id.
            fields (str): Requested properties

        Returs:
            dict: Json with the specified properties.

        """
        table_ref = self.bigquery_client.dataset(dataset_id, project_id).table(table_id)
        return self.bigquery_client.get_table(table_ref)
        return self._bigqueryService.tables().get(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id
        ).execute()

    def create_dataset(self, project_id, dataset_id, location):
        """Create a dataset in the specified project.

        Args:
            project_id (str): bigQuery project_id.
            dataset_id (str): dataset_id to create.

        Returns:
            bool: True if succesfully created. Raises error otherwhise.
        """
        #self.bigquery_client.location = location
        #parche
        try:
            dataset_ref = self.bigquery_client.dataset(dataset_id, project_id)
            dataset = self.bigquery_client.create_dataset(dataset_ref)
            return True
        except Exception:
            return True
        
