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
    def query(self):
        query = input('Enter query: ')
        return query
    def pull_query_data(self,query):
        df = pd.read_gbq(query=query,project_id=self.project_id)
        return df
    def read_csv(self,csv_path:str):
        #reading csv file
        df = pd.read_csv(csv_path)
        return df
    def read_excel(self,excel_path:str):
        #reading excel file
        df = pd.read_excel(excel_path)
        return df
    def read_json(self,json_path:str):
        #reading json file
        df = pd.read_json(json_path)
        return df
    def read_html(self,html_path:str):
        #reading html file
        df = pd.read_html(html_path)
        return df
    def read_sql(self,sql_path:str):
        #reading sql file
        df = pd.read_sql(sql_path)
        return df

