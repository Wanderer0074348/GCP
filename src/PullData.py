#importing all modules needed to pull data from bigquery
import pandas as pd
import numpy as np
from google.cloud import bigquery as bq
import pandas_gbq as pgbq

#creating a class to pull data from bigquery
class PullData():
    def __init__(self,project_id,dataset_id,table_id) -> None:
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = bq.Client()
    def pull_data(self):
        #pulling data from bigquery
        query = f"""
        SELECT *
        FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
        """
        df = pd.read_gbq(query=query,project_id=self.project_id)
        return df
    def head(self,df,n=5):
        #printing the head of the dataframe
        print(df.head(n))
    def tail(self,df,n=5):
        #printing the tail of the dataframe
        print(df.tail(n))
    def info(self,df):
        #printing the info of the dataframe
        print(df.info())
    def describe(self,df):
        #printing the describe of the dataframe
        print(df.describe())
    def shape(self,df):
        #printing the shape of the dataframe
        print(df.shape)
    def columns(self,df):
        #printing the columns of the dataframe
        print(df.columns)
    def index(self,df):
        #printing the index of the dataframe
        print(df.index)
    def dtypes(self,df):
        #printing the dtypes of the dataframe
        print(df.dtypes)
    def query(self):
        query = input('Enter query: ')
        return query
    def pull_query_data(self,query):
        df = pd.read_gbq(query=query,project_id=self.project_id)
        return df

