import os
import pandas as pd
import datetime


class Pipeline:
    
    """Function that will be used to extract data from json files 
    and get them ready for transformation
    """
    def extraction(self,year):
        dfs = []
        temp = pd.DataFrame()
        for root,dirs,files in os.walk('requests'+year):
            if files:
                for file in files:
                    data =pd.read_json(root+'\\'+file,lines = True)
                    dfs.append(data)
        temp = pd.concat(dfs,ignore_index=True)
        #print(temp.head())

    def orchestrate(self):
        directory = 'requests'
        for dir in next(os.walk(directory))[1]:
            print(dir)



#year = '\\2021'
instance = Pipeline()
#instance.extraction(year)
instance.orchestrate()