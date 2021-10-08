import os
from google.cloud.bigquery import schema, table
from google.cloud.bigquery.enums import WriteDisposition
import pandas as pd
from google.cloud import bigquery
from props.import_properties import Properties


class Pipeline:
    
    """Function that will be used to extract data from json files"""
    def __extraction(self,dir):
        print("entered extraction")
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
    def __transformation(self,df):
        print("entered transform")
        df = df[["method","path","endpoint","view-args","status","user","timestamp","browser","browser-platform","browser-version"]]
        df["method"] = df["method"].astype(str)
        df["path"] = df["path"].astype(str)
        df["endpoint"] = df["endpoint"].astype(str)
        df["view-args"] = df["view-args"].astype(str)
        df["user"] = df["user"].astype(str)
        df["browser"] = df["browser"].astype(str)
        df["browser-platform"] = df["browser-platform"].astype(str)
        df["browser-version"] = df["browser-version"].astype(str)
        df["year"] = pd.DatetimeIndex(df["timestamp"]).year
        df["month"] = pd.DatetimeIndex(df["timestamp"]).month
        df["day"] = pd.DatetimeIndex(df["timestamp"]).day
        df["hour"] = pd.DatetimeIndex(df["timestamp"]).hour
        df = df.rename(columns={"view-args": "view_args", "browser-platform": "browser_platform","browser-version":"browser_version"})
        return df

    """Function that loads the dataframe into google bigquery"""
    def __load(self,df,table,credentials):
        print("entered load")
        credentials_path = credentials
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

        client = bigquery.Client()
        table_id = table

        job_config = bigquery.LoadJobConfig(
            schema = [
                bigquery.SchemaField("method",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("path",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("endpoint",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("view_args",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("status",bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("user",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("timestamp",bigquery.enums.SqlTypeNames.TIMESTAMP),
                bigquery.SchemaField("browser",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("browser_platform",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("browser_version",bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("year",bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("month",bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("day",bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("hour",bigquery.enums.SqlTypeNames.INTEGER),
            ],
            write_disposition = "WRITE_APPEND",
        )
        job = client.load_table_from_dataframe(df,table_id,job_config=job_config)
        job.result()

        tab = client.get_table(table_id)
        print(
            "Loaded {} rows and {} columns to {}".format(
                tab.num_rows, len(tab.schema),table_id
            )
        )

    """Function to get credentials dynamically from a properties file"""
    def __get_credentials(self):
        props = Properties.get_conf_by_section('BigQueryCredentialsDocumentation')
        directory = props.get('directory')
        table = props.get('table_id')
        credentials = props.get('credentials_path')
        print(directory,table,credentials)
        return directory,table,credentials

    """Function that will orchestrate the entire pipeline"""
    def __orchestrate(self):
        print('entered orchestrate')
        directory,table, credentials = self.__get_credentials()
        for dir in next(os.walk(directory))[1]:
            path = directory+'\\'+dir
            df = self.__extraction(path)
            df = self.__transformation(df)
            print(df.head())
            self.__load(df,table,credentials)


    def __init__(self):
        self.__orchestrate()


instance = Pipeline()