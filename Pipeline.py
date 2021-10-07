import os
import pandas as pd
from google.cloud import bigquery


class Pipeline:
    
    """Function that will be used to extract data from json files"""
    def extraction(self,dir):
        dfs = []
        temp = pd.DataFrame()
        for root,dirs,files in os.walk(dir):
            if files:
                for file in files:
                    data = pd.read_json(root+'\\'+file,lines = True)
                    dfs.append(data)
        temp = pd.concat(dfs,ignore_index=True)
        return temp

    """Function that transforms the data"""
    def transformation(self,df):
        df = df[["method","path","endpoint","view-args","status","user","timestamp","browser","browser-platform","browser-version"]]
        df["year"] = pd.DatetimeIndex(df["timestamp"]).year
        df["month"] = pd.DatetimeIndex(df["timestamp"]).month
        df["day"] = pd.DatetimeIndex(df["timestamp"]).day
        df["hour"] = pd.DatetimeIndex(df["timestamp"]).hour
        return df

    def load(self):
        credentials_path = r'C:\pythonProjects\interview\CaseInterview.privatekey.json'
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

        client = bigquery.Client()
        table_id = 'caseinterview-328200.Log'

    """Function that will orchestrate the entire pipeline"""
    def orchestrate(self):
        directory = 'requests'
        for dir in next(os.walk(directory))[1]:
            path = directory+'\\'+dir
            df = self.extraction(path)
            df = self.transformation(df)
            self.load(df)
            #print(df.head())



#year = '\\2021'
instance = Pipeline()
#instance.extraction(year)
instance.orchestrate()