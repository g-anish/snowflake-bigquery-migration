from distutils.core import setup

from google.cloud import bigquery, storage
from google.oauth2 import service_account

from utils.time_utils import timeit


class GoogleCloudBaseHelper:
    def __init__(self, credentials_path: str, project_id: str):
        self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
        self.project_id = project_id

class BigQueryHelperGoogleCloud(GoogleCloudBaseHelper):
    def __init__(self, credentials_path: str, project_id: str):
        super().__init__(credentials_path, project_id)
        self.client = bigquery.Client(project=self.project_id, credentials=self.credentials)

    @timeit
    def load_data_from_gcs_to_bigquery(self, gcs_bucket_name: str, gcs_file_name: str, dataset_id: str, table_id: str):
        gcs_file_uri = f'gs://{gcs_bucket_name}/{gcs_file_name}/*'
        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)
        load_job = self.client.load_table_from_uri(gcs_file_uri, f'{dataset_id}.{table_id}', job_config=job_config)
        load_job.result()
        print(f'Data loaded successfully to {dataset_id}.{table_id}')

    @timeit
    def truncate_table(self, dataset_id: str, table_id: str):
        query = f'TRUNCATE TABLE `{dataset_id}.{table_id}`'
        query_job = self.client.query(query)
        query_job.result()
        print(f'Table {dataset_id}.{table_id} truncated successfully')

class CloudStorageHelperGoogleCloud(GoogleCloudBaseHelper):
    def __init__(self, credentials_path: str, project_id: str):
        super().__init__(credentials_path, project_id)
        self.storage_client = storage.Client(project=self.project_id, credentials=self.credentials)

    def delete_storge_folder(self, bucket_name: str, folder_name: str):
        bucket = self.storage_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=folder_name)
        for blob in blobs:
            blob.delete()
        print(f'Folder `{folder_name}` deleted successfully from `{bucket_name}`')
