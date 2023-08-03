import pandas as pd
import numpy as np
from google.cloud import bigquery as bq
import pandas_gbq as pgbq

class BigQueryUpdate:
    def __init__(self,project_id,dataset_id,table_id) -> None:
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = bq.Client()
    def create_table(self,df):
        #creating a table in bigquery
        pgbq.to_gbq(df,destination_table=f'{self.dataset_id}.{self.table_id}',project_id=self.project_id,if_exists='replace')
    def update_table(self,df):
        #updating a table in bigquery
        pgbq.to_gbq(df,destination_table=f'{self.dataset_id}.{self.table_id}',project_id=self.project_id,if_exists='append')
    def delete_table(self):
        #deleting a table in bigquery
        self.client.delete_table(f'{self.dataset_id}.{self.table_id}')
    def create_partitioned_table_on_date(self,df,part_column:str = 'date',exp_ms:int = 7776000000):
        #creating a partitioned table in bigquery
        #number of fields in the schema
        cols = list(df.columns)
        num_fields = len(cols)
        schema = []
        for col in range(0,num_fields):
            schema.append(bq.SchemaField(cols[col],cols[col]))
        table = bq.Table(f'{self.project_id}.{self.dataset_id}.{self.table_id}',schema=schema)
        table.time_partitioning = bq.TimePartitioning(type_=bq.TimePartitioningType.DAY,field=part_column,expiration_ms=exp_ms)
        table = self.client.create_table(table)
        print(f'Table {table.table_id} created')

bqu = BigQueryUpdate(project_id='quiet-axon-394009',dataset_id='testing_data_set',table_id='testing_table')
df = pd.DataFrame({'name':['John','Jane','Joe'],'age':[20,30,40],'date-of-birth':['2000-01-01','1990-01-01','1980-01-01']})
bqu.create_partitioned_table_on_date(df,part_column='date-of-birth',exp_ms=7776000000)