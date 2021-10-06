import os
import pandas as pd
import datetime



class Pipeline:
    
    def extraction(self):
        for root, dirs, files in os.walk('requests'):
            for file in files:
                print(root,file)

instance = Pipeline()
instance.extraction()