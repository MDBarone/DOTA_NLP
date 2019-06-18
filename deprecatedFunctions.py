from importData import arayi
from optimizeDB import renoa

class clyde(object):
    def addColumn(self,table,column,type):
        dota.dbExecute("ALTER TABLE DOTA.%s ADD COLUMN %s %s" % (table,column,type))
        print("Column added")

    def updateTableByName(self, table, column, data):
        assLang = 'UPDATE DOTA.%s SET %s = CASE ' % (table,column)

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
            dota.dbExecute(update1)
            print("Update 1 Success")
        except Exception as e:
            print("Update 1 Fail: %s" % e)
            # print(update1)
        try:
            dota.dbExecute(update2)
            print("Success 2")
        except Exception as e:
            print("Update 2 Fail: %s" % e)

    def createFKs(self):
        from itertools import combinations
        self.tables = dota.dbQuery("show tables in DOTA;")
        self.tables = [table[0] for table in self.tables if table[0] not in ["cluster_regions","player_rating", "ability_ids", "ability_upgrades","item_ids","hero_names"]]
        self.combo = combinations(self.tables,2)
        for com in self.combo:
            create = "ALTER TABLE DOTA.%s ADD FOREIGN KEY (match_id) REFERENCES DOTA.%s(match_id)" % (com[0], com[1])
            dota.dBQuery(create)
        dota.dbConnect().commit()

    def assignLang(self, langDict = {}):
        from langdetect import detect

        self.msgIdRange = dota.dbQuery("SELECT MIN(message_id), MAX(message_id) FROM DOTA.chat WHERE language IS NULL")
        self.msgIdRange = range(self.msgIdRange[0][0],self.msgIdRange[0][1]+1,10000)

        for idx, min in enumerate(self.msgIdRange):
            assLang = 'UPDATE DOTA.chat SET language = CASE '
            msgIdList = []
            whenThen = ''
            sentDict = {}
            if idx+1 == len(self.msgIdRange):
                self.data = dota.dbQuery("SELECT message_id, theKey FROM DOTA.chat WHERE message_id >= %s" % min)
            else:
                max = self.msgIdRange[idx+1]
                self.data = dota.dbQuery("SELECT message_id, theKey FROM DOTA.chat WHERE message_id BETWEEN %s AND %s" % (min, max))

            for row in self.data:
                message_id, message = row[0], row[1]
                try:
                    lang = detect(message)
                except:
                    continue
                whenThen += 'WHEN message_id = %s THEN "%s" ' % (message_id,lang)
                msgIdList.append(message_id)
            update = assLang + whenThen + ' ELSE %s END WHERE message_id in (%s)' % ("language",",".join(map(str,msgIdList)))
            dota.dbExecute(update)
            print("Commit. Updated at ID: %s " % (min))

        dota.addColumn("chat","language","VARCHAR (255)")
        dota.updateTableByName("chat","language",langDict)


    def assignAvgUserSent(self,sentDict={}):
        from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
        sid = SIA()
        self.data = dota.dbQuery("unit, theKey FROM DOTA.chat")

        for row in self.data:
            player, message = row[0], row[1]
            if player not in sentDict:
                sentDict[player]=[sid.polarity_scores(message)]
            else:
                sentDict[player].append(sid.polarity_scores(message))

        dota.addColumn("chat","valence","FLOAT")
        dota.updateTableByName("player","avg_valence",sentDict)
