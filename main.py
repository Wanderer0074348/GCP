#adding src path to sys.path
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

#adding data path to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), 'data'))
                             
#importing src modules
#importing data modules
from src.PullData import PullData
from src.Creds import Creds
from src.Actions import Actions

if __name__ == '__main__':
#creating an instance of PullData class
    creds = Creds()
    project_id,dataset_id,table_id = creds.retrieve_creds()
    pull_data = PullData(project_id = project_id,dataset_id = dataset_id,table_id = table_id)
#pulling data from bigquery 
    df = pull_data.pull_data()
    actions = Actions(df)
#pulling queried data from bigquery
    # df = pull_data.pull_query_data(query = pull_data.query())
#printing out the head of the dataframe
    pull_data.head(df)
#printing out the tail of the dataframe
    pull_data.tail(df)
#descibe the dataframe
    actions.dtypes() 