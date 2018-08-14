#!/usr/bin/env python3
import glob, csv, ast, sys, json, numpy as np
import pandas as pd
from langdetect import detect
from importDotaBase import arayi
from itertools import combinations

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
    def assignLang(self):
        pass

    # Add average player chat sentiment
    def avgUserSent(self):
        pass

optiDB = renoa()

optiDB.createFKs()
