import pandas as pd
from datetime import datetime
# import numpy as np
from google.cloud import bigquery as bq
import pandas_gbq as pgbq

class BigQueryUpdate:
    def __init__(self,project_id,dataset_id,table_id) -> None:
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = bq.Client()
    def create_empty_table(self,no_of_cols:int = 3):
        schema = []
        for col in range(0,no_of_cols):
            #input column names and types
            col_name = input('Enter column name: ')
            col_type = input('Enter column type: ').upper()
            col_perm = input('Enter column permission: ')
            schema.append(bq.SchemaField(col_name,col_type,col_perm))
        table = bq.Table(f'{self.project_id}.{self.dataset_id}.{self.table_id}',schema=schema)
        table = self.client.create_table(table)
        print(f'Table {table.table_id} created')
        
    def create_table_using_dataframe(self,df):
        #creating a table in bigquery
        pgbq.to_gbq(df,destination_table=f'{self.dataset_id}.{self.table_id}',project_id=self.project_id,if_exists='replace')
    def update_table_using_dataframe(self,df):
        #updating a table in bigquery
        pgbq.to_gbq(df,destination_table=f'{self.dataset_id}.{self.table_id}',project_id=self.project_id,if_exists='append')
    def delete_table(self):
        #deleting a table in bigquery
        self.client.delete_table(f'{self.dataset_id}.{self.table_id}')
    def create_empty_partitioned_table_on_date(self,exp_ms:int = 7776000000,no_of_cols:int = 3):
        #creating a partitioned table in bigquery
        #number of fields in the schema
        num_fields = no_of_cols
        schema = []
        for col in range(0,num_fields):
            #input column names and types
            col_name = input('Enter column name: ')
            col_type = input('Enter column type: ').upper()
            col_perm = input('Enter column permission: ')
            schema.append(bq.SchemaField(col_name,col_type,col_perm))
        col_partition = input('Enter partition column name: ')
        table = bq.Table(f'{self.project_id}.{self.dataset_id}.{self.table_id}',schema=schema)
        table.time_partitioning = bq.TimePartitioning(type_=bq.TimePartitioningType.DAY,field=col_partition,expiration_ms=exp_ms)
        table = self.client.create_table(table)
        print(f'Table {table.table_id} created')
    
    def create_partitioned_table_using_dataframe_on_time(self,df,exp_ms:int = 7776000000):
        schema = []
        for col in list(df.columns):
            print(col)
            col_type = input('Enter column type: ').upper()
            col_perm = input('Enter column permission: ')
            schema.append(bq.SchemaField(col,col_type,col_perm))
        print(schema)
        col_partition = input('Enter partition column name: ')
        job_config = bq.LoadJobConfig(schema=schema,time_partitioning=bq.TimePartitioning(type_=bq.TimePartitioningType.DAY,field=col_partition,expiration_ms=exp_ms))
        load_job = self.client.load_table_from_dataframe(df,f'{self.dataset_id}.{self.table_id}',job_config=job_config)

        result = load_job.result()
        print("Loaded {} rows into {}:{}.".format(result.output_rows, self.dataset_id, self.table_id))

bqu = BigQueryUpdate(project_id='quiet-axon-394009',dataset_id='testing_data_set',table_id='testing_table-3')
df = pd.DataFrame({'name':['John','Jane','Joe'],'age':[20,30,40],'date-of-birth':['2000-01-01','1990-01-01','1980-01-01']})
df["date-of-birth"] = pd.to_datetime(df["date-of-birth"],format='%Y-%m-%d')
# bqu.create_empty_table(no_of_cols=3)
# bqu.delete_table()
bqu.create_partitioned_table_using_dataframe_on_time(df,exp_ms=7776000000)