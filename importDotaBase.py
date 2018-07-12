#!/usr/bin/env python3
import glob, csv, ast, sys
import mysql.connector

class arayi(object):

    def dbConnect(self):
        cnx = mysql.connector.connect(user='root', password='root')
        return cnx

    ### EXTRACTS DB INFORMATION FOR IMPORT INTO SQL
    def getDBInfo(self,path):
        headerType=[]
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

    ### CREATE DATABASE AND TABLE SCHEMA
    def initDB(self,dbName):
        dB = dota.dbConnect()
        print ("INITIALIZE DB")
        self.cur = dB.cursor()
        self.cur.execute("CREATE DATABASE %s;" % dbName)
        self.cur.close()



    def deleteDB(self,dbName):
        dB = dota.dbConnect()
        print("DROP DB")
        self.cur = dB.cursor()
        self.cur.execute("DROP DATABASE %s;" % dbName)

    def createTable(self,schemaList):
        # PANDAS SOLUTION.  REFACTOR CODE
        # from sqlalchemy import create_engine
        # import pandas as pd
        #
        # df = pd.read_csv('/PATH/TO/FILE.csv', sep='|')
        # # Optional, set your indexes to get Primary Keys
        # df = df.set_index(['COL A', 'COL B'])
        #
        # engine = create_engine('mysql://user:pass@host/db', echo=False)

        df.to_sql(table_name, engine, index=False)
        ### SCHEMA LIST IS
        for table in schemaList:
            header,variables,types=dota.getDBInfo(table)
            varSchema=""
            pk=variables[0]
            for idx, value in enumerate(variables):
                if value == "key":
                    value="theKey"
                if types[idx] == str:
                    varSchema+=", \n %s %s" % (value, "VARCHAR(255)")
                else:
                    varSchema+=", \n %s %s" % (value, "FLOAT")

            createTable="CREATE TABLE DOTA.%s (%s);" % (header, varSchema.lstrip(','))
            print(createTable)
            dB = dota.dbConnect()
            self.cur = dB.cursor()
            self.cur.execute(createTable)
            print("Create table: %s" % header)
        pass



dota = arayi()
try:
    dota.initDB("DOTA")

    ### GETS LIST OF TABLES FOR IMPORT
    dotabaseList=glob.glob("data/*.csv")
    # print(dotabaseList)
    dota.createTable(dotabaseList)


    print("Success")
    dota.deleteDB("DOTA")
except EnvironmentError as e:
    dota.deleteDB("DOTA")
    print("Error %s:", e)
