#!/usr/bin/env python3
import glob, csv, ast, sys, json, numpy as np
import mysql.connector
import pandas as pd
from langdetect import detect

class melody(object):

    def dbConnect(self):
        cnx = mysql.connector.connect(user='root', password='root',database='DOTA')
        return cnx

    def dbQuery(self, cmd):
        self.dB = tilt.dbConnect()
        self.df = pd.read_sql(cmd, con=self.dB)
        return self.df


tilt = melody()


### ENGLISH ANALYSIS TEST
# DUMP JSON INTO TEMPFILE FOR NOW
# query = "SELECT unit, theKey FROM chat"
# df = tilt.dbQuery(query)
# userLog = {}
#
# for row in df.itertuples(index=True,name="Pandas"):
#     player, message = getattr(row,"unit"), getattr(row,"theKey")
#     try:
#         lang = detect(message)
#         if lang not in ["en"]:
#             continue
#     except:
#         continue
#
#     if player not in userLog:
#         userLog[player]=[message]
#     else:
#         userLog[player].append(message)
#
# json.dump(userLog,open('data/summaries/englishChatTest.json','w'))
##
# userLog = json.load(open("data/summaries/englishChatTest.json","r"))
# avgChat=[]
# for user in userLog:
#     avgChat.append(len(userLog[user]))
#
# print(np.mean(avgChat))



#########
# query = "SELECT theKey FROM chat"
# df = tilt.dbQuery(query)
# langDict = {}
#
# for row in df.itertuples(index=True,name="Pandas"):
#     message = getattr(row,"theKey")
#     try: detect(message)
#     except: continue
#     lang = str(detect(message))
#     if lang not in langDict:
#         langDict[lang]=1
#     else:
#         langDict[lang]+=1
#
# print(langDict)
# json.dump(langDict,open('data/summaries/languageSummary.json','w'))

import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt

langLog = json.load(open("data/summaries/languageSummary.json","r"))

plt.bar(range(len(langLog)), list(langLog.values()), align='center')
plt.xticks(range(len(langLog)), list(langLog.keys()))
# matplotlib.axis.XAxis(axes,labelpad=20)
plt.savefig("data/images/matplotlib.png")
