import os
import pandas as pd
import datetime



class Pipeline:
    
    """Function that will be used to extract data from json files 
    and get them ready for transformation
    """
    def extraction(self):
        for root, dirs, files in os.walk('requests'):
            for file in files:
                print(root,file)

instance = Pipeline()
instance.extraction()