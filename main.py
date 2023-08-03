#adding src path to sys.path
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

#importing src modules
from src.PullData import PullData

#creating an instance of PullData class
pull_data = PullData(project_id='quiet-axon-394009',dataset_id='testing_data_set',table_id='international_tourist_trips')




if __name__ == '__main__':
#pulling data from bigquery
    df = pull_data.pull_data()
#printing out the head of the dataframe
    pull_data.head(df)
#printing out the tail of the dataframe
    pull_data.tail(df)