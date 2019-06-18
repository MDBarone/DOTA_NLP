#!/usr/bin/env python3
import glob, csv, sys, json
import numpy as np
import mysql.connector
import pandas as pd
from importData import arayi

dota = arayi()

# query = "SELECT m.match_id, CASE WHEN c.slot <= 4 AND m.radiant_win = 'True' THEN 'win' ELSE 'lose' END AS team, count(*) theCount, AVG(JSON_EXTRACT(c.valence, '$.pos')) as pos, AVG(JSON_EXTRACT(c.valence, '$.neg')) as neg, AVG(JSON_EXTRACT(c.valence, '$.neu')) as neu, AVG(JSON_EXTRACT(c.valence, '$.compound')) as comp FROM DOTA.match as m, DOTA.player as p, DOTA.chat as c WHERE c.player_id = p.player_id AND c.match_id = m.match_id AND p.language = 'en' GROUP BY m.match_id, team ORDER BY m.match_id, team"
# df = pd.read_sql(query, con=dota.dbConnect())
# df.to_csv("data/temp.csv")

query = "SELECT CASE WHEN c.slot <= 4 AND m.radiant_win = 'True' THEN 'win' ELSE 'lose' END AS team, GROUP_CONCAT(c.theKey SEPARATOR ' ') as chatLog FROM %s.chat as c, %s.match as m, %s.player as p WHERE c.player_id = p.player_id AND c.match_id = m.match_id AND p.language = 'en' GROUP BY team" % (config.MYSQL_DATABASE,config.MYSQL_DATABASE,config.MYSQL_DATABASE)
df = pd.read_sql(query, con=dota.dbConnect())
df.to_csv("data/temp.csv")


# for row in data:
#     match,team, val_neg, val_neu, val_pos, val_comp = row[1:6]
    # if (slot <= 4 and outcome == "True") or (slot > 4 and outcome == "False"):
#         # print("Win:", slot, outcome)
#         print(type(valence),valence)
#         pass
#     else:
#         pass
        # print("Loss:",slot,outcome)
        # lost

# class clyde(object):
#
#     def dbConnect(self):
#         cnx = mysql.connector.connect(user='root', password='fantasy3',database='DOTA')
#         return cnx
#
#     def dbQuery(self, cmd):
#         self.dB = tilt.dbConnect()
#         self.df = pd.read_sql(cmd, con=self.dB)
#         return self.df


# tilt = clyde()


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

# import matplotlib
# matplotlib.use('agg')
# import matplotlib.pyplot as plt
#
# langLog = json.load(open("data/summaries/languageSummary.json","r"))
#
# plt.bar(range(len(langLog)), list(langLog.values()), align='center')
# plt.xticks(range(len(langLog)), list(langLog.keys()))
# # matplotlib.axis.XAxis(axes,labelpad=20)
# plt.savefig("data/images/matplotlib.png")


#### CORRELATE AVG SENTIMENT TO WIN PERCENTAGE (FIGURE OUT A WAY TO QUANTIFY THAT)
