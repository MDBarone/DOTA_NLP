import json, sys
import pandas as pd
# import numpy as np
# import math
# import itertools
# import re
# import scipy.stats as sp
from operator import itemgetter as it

class musei(object):

    #Loads a hard-coded slack message.
    ## Should access data via API request from a different source (JS?)
    def loadMatchData(self,pathToMessage,latest):
        with open(pathToMessage,"r") as f:
            try:
                self.response=json.load(f)
                if self.response["ok"] is True and self.response["latest"] > latest:
                    archive=sorted(self.response["messages"],key=it("ts"))
                    userList=set([msg["user"] for msg in archive])
                    return archive
                    # for msg in archive:
                        # print msg["user"],msg["ts"],msg["text"]

            except:
                print("Unexpected error:",sys.exc_info()[4])

    def createMatchToPlayerDict(self,csvPath):
        df=pd.read_csv(csvPath)
        matchPlayerDict = {int(row):[] for row in df["match_id"].tolist()}
        return matchPlayerDict

    def getAcctToPlyrIds(self,matchIds):
        playerDf=pd.read_csv("/Users/arayi/DOTA-Science/players.csv")
        playerDf=playerDf.replace({"player_slot":{128:5,129:6,130:7,131:8,132:9}})
        print(playerDf)
        userId={}
        for match in matchIds:
            matchDf=playerDf.loc[playerDf["match_id"] == match,["account_id","player_slot"]]
            matchDf=matchDf.loc[playerDf["account_id"] != 0]
            userId[match]=matchDf.set_index("player_slot").to_dict()["account_id"]
        return userId

    def getChatsFromAcct(self,matchDict):
        archiveDf=pd.read_csv("/Users/arayi/DOTA-Science/chat.csv")
        chatDict={}
        for match in matchDict:
            chatDf=archiveDf.loc[archiveDf["match_id"] == match,["time","slot","unit","key"]]
            for idx, row in chatDf.iterrows():
                if row[1] not in matchDict[match].keys():
                    continue
                acctId=matchDict[match][row[1]]
                if acctId not in chatDict:
                    chatDict[acctId]=[{"match":match,"time":row[0],"message":row[3]}]
                else:
                    chatDict[acctId].append({"match":match,"time":row[0],"message":row[3]})
        return chatDict



m = musei()
matchPlyrDict = m.getAcctToPlyrIds(range(0,50000))
print(matchPlyrDict)
chatDict = m.getChatsFromAcct(matchPlyrDict)
json.dump(chatDict,open("chatData.json","w"))
