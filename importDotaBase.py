#!/usr/bin/env python3
import glob, csv, ast

class arayi(object):
    def getDBInfo(self,path):
        headerType=[]
        print(path)
        # Is there a better way to do this line?
        tableName=path.replace("data/","").split(".csv")[0]
        with open(path,"r") as f:
            reader = csv.reader(f)
            header = next(reader)
            reader=list(reader)
            for head in reader[0]:
                try:
                    value = ast.literal_eval(head)
                    headerType.append(type(value))
                except:
                    headerType.append(type(head))
        return [tableName, header, headerType]

dota = arayi()
dotabaseList=glob.glob("data/*.csv")
for table in dotabaseList:
    print(dota.getDBInfo(table))

###CREATE DB



### FIND CSVS, MAKE TABLE WITH COLUMNS AND IMPORT
