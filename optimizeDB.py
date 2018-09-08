#!/usr/bin/env python3
import glob, csv, ast, sys, json, numpy as np
import pandas as pd
from langdetect import detect
from importDotaBase import arayi
from itertools import combinations
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA

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
            db.commit()

        self.cur.close()

        pass

    # Add player primary language column
    def assignLang(self,langDict={}):
        dB = dota.dbConnect()
        self.cur = dB.cursor()
        self.cur.execute("player_name, message FROM DOTA.chat")
        data= self.cur.fetchall()
        for row in data:
            if row[0] not in langDict:
                try:
                    langDict[row[0]]=detect(row[1])
                except:
                    continue
        addColumn="ALTER TABLE DOTA.chat ADD COLUMN language VARCHAR(255)"
        try:
            self.cur.execute(addColumn)
        except:
            self.cur.execute("ALTER TABLE DOTA.chat DROP COLUMN language")
        dB.commit()
        # assLang="UPDATE TABLE chat SET language = '%s' "
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

optiDB = renoa()

# optiDB.createFKs() # Might not work
# optiDB.assignLang() # Run next NEEDS WORK
