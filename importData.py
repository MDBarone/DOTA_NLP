#!/usr/bin/env python3
import glob, csv, ast, sys
import mysql.connector
import config # config.py file contains all passwords and credentials

class arayi(object):

    def dbConnect(self):
        cnx = mysql.connector.connect(host='localhost',user=config.MYSQL_USER, password=config.MYSQL_PASSWORD,allow_local_infile=True,database="DOTA")
        return cnx

    def dbQuery(self,query):
        self.dB = dota.dbConnect()
        self.cur = self.dB.cursor()
        self.cur.execute(query)
        try:
            self.data = self.cur.fetchall()
        except:
            self.data = "Completed query with no data: %s"
        self.cur.close()
        return self.data

    def dbExecute(self,command):
        self.dB = dota.dbConnect()
        self.cur = self.dB.cursor()
        self.cur.execute(command)
        self.dB.commit()
        self.cur.close()

    ### EXTRACTS DB INFORMATION FOR IMPORT INTO SQL
    def getDBInfo(self,path):
        headerType=[]
        tableName=path.replace("data/","").split(".csv")[0]
        with open(path,"r") as f:
            reader = csv.reader(f)
            header = next(reader)
            reader = list(reader)
            for head in reader[0]:
                try:
                    value = ast.literal_eval(head)
                    headerType.append(type(value))
                except:
                    headerType.append(type(head))
        return [tableName, header, headerType]

    ### CREATE DATABASE AND TABLE SCHEMA
    def initDB(self,dbName):
        print ("INITIALIZE DB")
        dota.dbExecute("CREATE DATABASE %s;" % dbName)

    def deleteDB(self,dbName):
        dota.dbExecute("DROP DATABASE %s;" % dbName)

    def importTable(self,schemaList):
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
                elif types[idx] == bool:
                    varSchema+=", \n %s %s" % (value, "ENUM('True','False')")
                else:
                    varSchema+=", \n %s %s" % (value, "FLOAT")

            createTable="CREATE TABLE %s.%s (%s);" % (config.MYSQL_DATABASE, tableName, varSchema.lstrip(','))
            print("Create table: %s" % tableName)
            uploadCommand="LOAD DATA LOCAL INFILE '%s' INTO TABLE %s.%s FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS" % (table, config.MYSQL_DATABASE, tableName)
            print(uploadCommand)
            dota.dbExecute(uploadCommand)

    def updateTableByID(self, table, column, data):
        assLang = 'UPDATE %s.%s SET %s = CASE ' % (config.MYSQL_DATABASE, table, column)
        playerList = []
        whenThen = ''
        for theId in data:
            whenThen += "WHEN message_id = %s THEN %s " % (theId, data[theId])
            playerList.append(theId)
        update = assLang + whenThen + " ELSE %s END WHERE message_id in (%s)" % (column,",".join(map(str,playerList)))
        dota.dbExecute(update)


dota = arayi()

# ###DB INITAILIZATION
# dbName = "DOTA"
# try:
#     dota.dbExecute("CREATE DATABASE %s;" % dbName)
#     # GETS LIST OF TABLES FOR IMPORT
#     dotabaseList=glob.glob("data/*.csv")

#     # CREATES TABLE SCHEMAS AND UPLOADS DATA FROM data/
#     # print(dotabaseList)
#     dota.importTable(dotabaseList)
#     print("DATA IMPORT SUCCESS")
# except EnvironmentError as e:
#     dota.dbExecute("DROP DATABASE %s;" % dbName)
#     print("DATA IMPORT ERROR: %s", e)

### INDIVIDUAL TABLE IMPORT
# match_schema=glob.glob("data/match.csv")
# dota.importTable(match_schema)
