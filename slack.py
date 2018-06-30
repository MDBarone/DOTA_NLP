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
    def loadDemoMessage(self,pathToMessage,latest):
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
                print "Unexpected error:",sys.exc_info()[4]



    def parseMessage(self,slackHistory):
        pass

m=musei()

slack_msg="/home/saradit/musei/data/dota2/slack_demo.txt"
watson_out=""
last="1512085950.000216"
m.loadDemoMessage(msg,last)