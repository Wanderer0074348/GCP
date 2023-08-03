#adding src path to sys.path
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

#importing src modules
from src.PullData import PullData





if __name__ == '__main__':
#creating an instance of PullData class
    project_id = input('Enter project id: ')
    dataset_id = input('Enter dataset id: ')
    table_id = input('Enter table id: ')
    pull_data = PullData(project_id = project_id,dataset_id = dataset_id,table_id = table_id)
#pulling data from bigquery
    df = pull_data.pull_data()
#printing out the head of the dataframe
    pull_data.head(df)
#printing out the tail of the dataframe
    pull_data.tail(df)