#!/usr/bin/env python3
import glob, csv, ast, sys
import mysql.connector

class arayi(object):

    def dbConnect(self):
        cnx = mysql.connector.connect(user='root', password='fantasy3',allow_local_infile=True)
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
        for table in schemaList:
            tableName,variables,types=dota.getDBInfo(table)
            varSchema=""
            pk=variables[0]
            for idx, value in enumerate(variables):

                ### EXCEPTION BECAUSE MYSQL RESTRICTS USING KEY AS A KEYNAME
                if value == "key":
                    value="theKey"

                if types[idx] == str:
                    varSchema+=", \n %s %s" % (value, "VARCHAR(255)")
                else:
                    varSchema+=", \n %s %s" % (value, "FLOAT")
            # varSchema+=", \n PRIMARY KEY (%s)" % variables[0]
            createTable="CREATE TABLE DOTA.%s (%s);" % (tableName, varSchema.lstrip(','))
            # print(createTable)
            dB = dota.dbConnect()
            self.cur = dB.cursor()
            self.cur.execute(createTable)
            print("Create table: %s" % tableName)
            print(createTable)
            uploadCommand="LOAD DATA LOCAL INFILE '%s' INTO TABLE DOTA.%s FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS" % (table, tableName)
            print(uploadCommand)
            self.cur.execute(uploadCommand)
            dB.commit()

        ### THIS IS A MORE ELEGANT SOLUTION I COULDNT GET TO WORK
        # import pandas as pd
        # from sqlalchemy import create_engine
        # for table in schemaList:
        #     df = pd.read_csv(table, sep=',')
        #     pk = list(df)[0]
        #     tableName = table.replace(".csv","").split("/")[1]
        #     df = df.set_index(pk)
        #     # engine = create_engine('mysql://user:pass@host/db', echo=False)
        #     engine = create_engine('mysql://root:root@localhost/DOTA', echo=False)
        #     try:
        #         df.to_sql(tableName, engine, index=False)
        #     except EnvironmentError as e:
        #         dota.deleteDB("DOTA")
        #         print("CREATE Error %s:", e)
        #     break

        pass


dota = arayi()
dota.deleteDB("DOTA")
##DB INITAILIZATION
try:
    dota.initDB("DOTA")
    ### GETS LIST OF TABLES FOR IMPORT
    dotabaseList=glob.glob("data/*.csv")
    # CREATES TABLE SCHEMAS AND UPLOADS DATA FROM data/
    # print(dotabaseList)
    dota.createTable(dotabaseList)
    # dota.deleteDB("DOTA")
    print("DATA IMPORT SUCCESS")
except EnvironmentError as e:
    dota.deleteDB("DOTA")
    print("DATA IMPORT ERROR: %s", e)
