import pandas as pd
import numpy as np

class Actions:
    def __init__(self,df) -> None:
        self.df = df
    def info(self):
        #printing the info of the dataframe
        print(self.df.info())
    def describe(self):
        #printing the describe of the dataframe
        print(self.df.describe())
    def shape(self):
        #printing the shape of the dataframe
        print(self.df.shape)
    def columns(self):
        #printing the columns of the dataframe
        print(self.df.columns)
    def index(self):
        #printing the index of the dataframe
        print(self.df.index)
    def dtypes(self):
        #printing the dtypes of the dataframe
        print(self.df.dtypes)
    def to_csv(self,path:str):
        #saving the dataframe to csv
        self.df.to_csv(path)
    def to_excel(self,path:str):
        #saving the dataframe to excel
        self.df.to_excel(path)
    def to_json(self,path:str):
        #saving the dataframe to json
        self.df.to_json(path)