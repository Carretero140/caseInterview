from configparser import ConfigParser
#import os 

config = ConfigParser()
config.read(r'interview\props\conf.properties')
class Properties:

    def get_conf_by_section(section:str):
        r= {}
        for item in config.items(section):
            r[item[0]] = item[1]
        return r


# for root, dirs, files in os.walk("."): 
#     for d in dirs:
#         print(os.path.relpath(os.path.join(root, d), "."))
#     for f in files: 
#         print(os.path.relpath(os.path.join(root, f), "."))