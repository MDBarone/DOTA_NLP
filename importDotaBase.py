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

    def addColumn(self,table,column,type):
        dota.dbQuery("ALTER TABLE DOTA.%s ADD COLUMN %s %s" % (table,column,type))
        print("Column added")

    ## Usually better to use updateTableByID
    # SOME
    def updateTableByName(self, table, column, data):
        self.dB = dota.dbConnect()
        self.cur = self.dB.cursor()
        assLang = 'UPDATE DOTA.%s SET %s = CASE ' % (table,column)

        # I need to make two updates because some player names have single or double quotes
        playerList1 = []
        playerList2 = []
        whenThen1 = ''
        whenThen2 = ''

        for unit in data:
            try:
                if '"' in unit:
                    whenThen1 += "WHEN unit = '%s' THEN '%s' " % (unit, data[unit])
                    playerList1.append(unit)
                else:
                    whenThen2 += 'WHEN unit = "%s" THEN "%s" ' % (unit, data[unit])
                    playerList2.append(unit)
            except Exception as e:
                print(e)
                continue
        update1 = assLang + whenThen1 + " ELSE %s END WHERE unit in ('%s')" % (column,"','".join(map(str,playerList1)))
        update2 = assLang + whenThen2 + ' ELSE %s END WHERE unit in ("%s")' % (column,'","'.join(map(str,playerList2)))
        try:
            self.cur.execute(update1)
            print("Update 1 Success")
        except Exception as e:
            print("Update 1 Fail: %s" % e)
            # print(update1)
        try:
            self.cur.execute(update2)
            print("Success 2")
        except Exception as e:
            print("Update 2 Fail: %s" % e)

        self.dB.commit()
        self.cur.close()

    def updateTableByID(self, table, column, data):
        # Once player table is built, player_ids will be easier to use
        assLang = 'UPDATE DOTA.%s SET %s = CASE ' % (table,column)

        playerList = []
        whenThen = ''

        for theId in data:
            whenThen += "WHEN message_id = %s THEN %s " % (theId, data[theId])
            playerList.append(theId)
        update = assLang + whenThen + " ELSE %s END WHERE message_id in (%s)" % (column,",".join(map(str,playerList)))
        dB = dota.dbConnect()

        dota.dbQuery(update)
        dB.commit()
        # dB = dota.dbConnect()
        # self.cur = dB.cursor()
        # self.cur.execute(update)
        # dB.commit()
        # self.cur.close()

dota = arayi()



# ###DB INITAILIZATION
# try:
#     dota.initDB("DOTA")
#     # GETS LIST OF TABLES FOR IMPORT
#     dotabaseList=glob.glob("data/*.csv")

#     # CREATES TABLE SCHEMAS AND UPLOADS DATA FROM data/
#     # print(dotabaseList)
#     dota.importTable(dotabaseList)
#     print("DATA IMPORT SUCCESS")
# except EnvironmentError as e:
#     dota.deleteDB("DOTA")
#     print("DATA IMPORT ERROR: %s", e)

### INDIVIDUAL TABLE IMPORT
# match_schema=glob.glob("data/match.csv")
# dota.importTable(match_schema)
