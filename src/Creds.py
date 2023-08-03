import pandas as pd


class Creds:
    def __init__(self) -> None:
        pass
    def retrieve_creds(self):
        df = pd.read_json("/home/tanay/Documentos/GCP/data/creds.json")
        return [df["credentials"].iloc[1],df["credentials"].iloc[0],df["credentials"].iloc[2]]
    #changing the credentials
    def change_creds(self):
        project_id = input('Enter new project id: ')
        dataset_id = input('Enter new dataset id: ')
        table_id = input('Enter new table id: ')
        df = pd.DataFrame({'credentials':[project_id,dataset_id,table_id]})
        df.to_json("/home/tanay/Documentos/GCP/data/creds.json")
