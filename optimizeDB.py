#!/usr/bin/env python3
import glob, sys, json
import pandas as pd
from importDotaBase import arayi


dota = arayi()

class renoa(object):

    def createIndexes(self):
        # < 50 messages got replicated during the import/optimize.
        data = dota.dbQuery("SELECT count(*) as c, message_id from DOTA.chat GROUP BY message_id ORDER BY c DESC LIMIT 100;")
        data = [d[1] for d in data if d[0] > 1]
        delQuery = "DELETE FROM chat WHERE message_id IN (%s)" % ",".join(map(str,data))
        dota.dbExecute(delQuery)
        # print(delQuery)

        pks = [("chat","message_id")]
        [dota.dbExecute("ALTER TABLE DOTA.%s ADD PRIMARY KEY (%s);" % (pk[0],pk[1])) for pk in pks]

        indexes = [("chat","player_id"),("chat","match_id")]
        [dota.dbExecute("CREATE INDEX idx_%s ON DOTA.%s(%s);" % (ind[1],ind[0],ind[1])) for ind in indexes]

    # Will create foreign keys for all tables with match ids dynamically
    # NEEDS WORK
    # def createFKs(self):
    #     from itertools import combinations
    #     self.tables = dota.dbQuery("show tables in DOTA;")
    #     self.tables = [table[0] for table in self.tables if table[0] not in ["cluster_regions","player_rating", "ability_ids", "ability_upgrades","item_ids","hero_names"]]
    #     self.combo = combinations(self.tables,2)
    #     for com in self.combo:
    #         create = "ALTER TABLE DOTA.%s ADD FOREIGN KEY (match_id) REFERENCES DOTA.%s(match_id)" % (com[0], com[1])
    #         dota.dBQuery(create)
    #     dota.dbConnect().commit()


    # Add player primary language column
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

        # dota.addColumn("chat","language","VARCHAR (255)")
        # dota.updateTableByName("chat","language",langDict)

    def assignSent(self, sentDict = {}):
        from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
        sid = SIA()
        dB = dota.dbConnect()
        self.msgIdRange = dota.dbQuery("SELECT MIN(message_id), MAX(message_id) FROM DOTA.chat WHERE valence IS NULL")
        self.msgIdRange = range(self.msgIdRange[0][0],self.msgIdRange[0][1]+1,10000)
        self.cur = dB.cursor()
        for idx, min in enumerate(self.msgIdRange):
            assLang = 'UPDATE DOTA.chat SET valence = CASE '
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
                scores = sid.polarity_scores(message)
                jsonObject = ''
                for val in zip(scores.keys(),scores.values()):
                    jsonObject += "'%s', %s, " % (val[0], val[1])
                whenThen += 'WHEN message_id = %s THEN JSON_OBJECT(%s) ' % (message_id, jsonObject.rstrip(', '))
                msgIdList.append(message_id)
            update = assLang + whenThen + ' ELSE %s END WHERE message_id in (%s)' % ("valence",",".join(map(str,msgIdList)))
            # print(update)
            dota.dbExecute(update)
            print("Commit. Updated at ID: %s " % (min))
        print("Language Assigned.")

    # Add average player chat sentiment
    # def assignAvgUserSent(self,sentDict={}):
    #     from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
    #     sid = SIA()
    #     self.data = dota.dbQuery("unit, theKey FROM DOTA.chat")
    #
    #     for row in self.data:
    #         player, message = row[0], row[1]
    #         if player not in sentDict:
    #             sentDict[player]=[sid.polarity_scores(message)]
    #         else:
    #             sentDict[player].append(sid.polarity_scores(message))

        # dota.addColumn("chat","valence","FLOAT")
        # dota.updateTableByName("player","avg_valence",sentDict)


    def createPlayer(self):
        # This can be improved with some functions
        dB = dota.dbConnect()
        self.cur = dB.cursor()
        print("Connection established")

        query = 'CREATE TABLE player(unit VARCHAR(255), total_messages INT, val_neu FLOAT, val_pos FLOAT, val_neg FLOAT, val_compound FLOAT);'
        dota.dbExecute(query)
        print("Player Temp created")

        query = 'INSERT INTO player SELECT unit, count(*) as total_messages, AVG(JSON_EXTRACT(valence, "$.neu")) AS val_neu, AVG(JSON_EXTRACT(valence, "$.pos")) AS val_pos, AVG(JSON_EXTRACT(valence, "$.neg")) AS val_neg, AVG(JSON_EXTRACT(valence, "$.compound")) AS val_compound FROM DOTA.chat GROUP BY unit;'
        dota.dbExecute(query)
        print("Player Temp data migrated")

        query = "ALTER TABLE DOTA.player ADD COLUMN player_id INT AUTO_INCREMENT PRIMARY KEY, ADD COLUMN avg_valence JSON"
        dota.dbExecute(query)
        print("Columns added")

        query = "CREATE TABLE chat_tmp AS SELECT c.*, p.player_id FROM DOTA.chat as c, DOTA.player as p WHERE c.unit = p.unit; DROP TABLE chat; ALTER TABLE chat_tmp RENAME TO chat;"
        dota.dbExecute(query)
        print("Player ID added to chat table.")

        query = "SELECT player_id, val_neu, val_neg, val_pos, val_compound FROM player"
        data = dota.dbQuery(query)
        convertDict = {}
        assLang = 'UPDATE DOTA.player SET avg_valence = CASE '
        idList = []
        whenThen = ''

        for row in data:
            convertValues = "JSON_OBJECT('neu', %s, 'neg', %s, 'pos', %s, 'compound', %s)" % (row[1], row[2], row[3], row[4])
            whenThen += "WHEN player_id = %s THEN %s " % (row[0], convertValues)
            idList.append(row[0])

        update = assLang + whenThen + " ELSE avg_valence END WHERE player_id in (%s)" % (",".join(map(str,idList)))
        dota.dbExecute(query)

    def groupLang(self):
        from langdetect import detect

        self.data = dota.dbQuery("SELECT player_id, GROUP_CONCAT(chat.theKey SEPARATOR '.  ') FROM DOTA.chat GROUP BY chat.player_id")
        assLang = 'UPDATE DOTA.player SET language = CASE '
        idList = []
        whenThen = ''

        for row in self.data:
            try:
                lang = detect(row[1])
                whenThen += "WHEN player_id = %s THEN '%s' " % (row[0], lang)
                idList.append(row[0])
            except Exception as e:
                print("Error - %s: %s" % (e,row[1]))
        update = assLang + whenThen + " ELSE language END WHERE player_id in (%s)" % (",".join(map(str,idList)))
        dota.dbExecute(update)


optiDB = renoa()

# try:
    # optiDB.groupLang()
    # optiDB.assignSent()
    # optiDB.createPlayer()
    # optiDB.createIndexes()
# except Error as e:
    # print("Error: %s" % e)
