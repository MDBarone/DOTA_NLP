#!/usr/bin/env python3
import glob, csv, ast, sys, json, numpy as np
import pandas as pd
from langdetect import detect
from importDotaBase import arayi
from itertools import combinations
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
import regex as re

dota = arayi()

class renoa(object):

    # Will create foreign keys for all tables with match ids dynamically
    def createFKs(self):
        dB = dota.dbConnect()
        self.cur = dB.cursor()
        self.cur.execute("show tables in DOTA;")
        tables = self.cur.fetchall()
        tables = [table[0] for table in tables if table[0] not in ["cluster_regions","player_rating", "ability_ids", "ability_upgrades","item_ids","hero_names"]]
        combo = combinations(tables,2)
        for com in combo:
            create = "ALTER TABLE DOTA.%s ADD FOREIGN KEY (match_id) REFERENCES DOTA.%s(match_id)" % (com[0], com[1])
            print(create)
            self.cur.execute(create)
            dB.commit()

        self.cur.close()

        pass

    # Add player primary language column
    def assignLang(self, langDict = {}):
        regex = re.compile('[@_!#$%^&*()<>?/\|}{~:]')
        dB = dota.dbConnect()
        self.cur = dB.cursor()
        self.cur.execute("SELECT unit, theKey FROM DOTA.chat")
        data = self.cur.fetchall()
        for row in data:
            player = row[0]
            if type(player) == None:
                continue
            if player not in langDict:
                try:
                    langDict[player] = detect(row[1])
                except:
                    continue

        # addColumn = "ALTER TABLE DOTA.chat ADD COLUMN language VARCHAR(255)"
        # try:
        #     self.cur.execute(addColumn)
        #     print("Column added")
        # except Exception as e:
        #     self.cur.execute("ALTER TABLE DOTA.chat DROP COLUMN language")
        #     print("Column fail: %s" % e)

        dB.commit()
        assLang = 'UPDATE DOTA.chat SET language = CASE '
        playerList1 = []
        playerList2 = []
        whenThen1 = ''
        whenThen2 = ''
        for player in langDict:
            try:
                if ("'" in player and '"' in player):
                    # print("Fail: %s" % player)
                    continue
                elif '"' in player:
                    whenThen1 += "WHEN unit = '%s' THEN '%s' " % (player, langDict[player])
                    playerList1.append(player)
                    # whenThen = 'WHEN unit = "%s" THEN "%s" ' % (player, langDict[player])
                else:
                    if regex.search(player) == None:
                        whenThen2 += 'WHEN unit = "%s" THEN "%s" ' % (player, langDict[player])
                        playerList2.append(player)
            except:
                continue
        update1 = assLang + whenThen1 + " ELSE language END WHERE unit in ('%s')" % "','".join(map(str,playerList1))
        update2 = assLang + whenThen2 + ' ELSE language END WHERE unit in ("%s")' % '","'.join(map(str,playerList2))
        # try:
        #     self.cur.execute(update1)
        # except Exception as e:
        #     print("Fail1: %s" % e)
        #     # print(update1)
        try:
            self.cur.execute(update2)
        except Exception as e:
            print("Fail2: %s" % e)
            # print(update2)
        dB.commit()
        self.cur.close()
        # return langDict

    # Add average player chat sentiment
    def assignAvgUserSent(self,sentDict={}):
        sid = SIA()
        dB = dota.dbConnect()
        self.cur = dB.cursor()
        self.cur.execute("player_name, message FROM DOTA.chat")
        data= self.cur.fetchall()
        for row in data:
            if row[0] not in sentDict:
                sentDict[row[0]]=[sid.polarity_scores(sentence)]
            else:
                sentDict[row[0]].append(sid.polarity_scores(sentence))
        pass

    def populatePlayer(self):
        # query = "CREATE TABLE player SELECT DISTINCT unit, match_id FROM DOTA.chat;"
        dB = dota.dbConnect()
        self.cur = dB.cursor()
        query = "SELECT unit, row_number() OVER (ORDER BY unit) FROM (SELECT DISTINCT unit FROM DOTA.player WHERE plyr_id IS NULL) t;"
        self.cur.execute(query)
        data = self.cur.fetchall()
        # print(data)
        for cell in data:
            unit = cell[0]
            player_id = cell[1]

            if unit is None:
                print("None!")
                continue
            if '"' in unit:
                update = "UPDATE DOTA.player SET plyr_id = %s WHERE unit = '%s'" % (player_id,unit)
            else:
                update = 'UPDATE DOTA.player SET plyr_id = %s WHERE unit = "%s"' % (player_id,unit)
            # print(update)
            try:
                self.cur.execute(update)
            except:
                print("FAIL: %s, %s" % (unit, player_id))
                continue
        dB.commit()
        # pass

optiDB = renoa()

# optiDB.createFKs() # Might not work
optiDB.assignLang() # Run next NEEDS WORK
# optiDB.populatePlayer()
