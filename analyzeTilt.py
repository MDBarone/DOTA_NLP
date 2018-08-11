#!/usr/bin/env python3
import glob, csv, ast, sys
import mysql.connector
import pandas as pd

class melody(object):

    def dbConnect(self):
        cnx = mysql.connector.connect(user='root', password='root',database='DOTA')
        return cnx

    def dbQuery(self, cmd):
        self.dB = tilt.dbConnect()
        self.df = pd.read_sql(cmd, con=dB)
        return self.df


tilt = melody()
query = "SELECT match_id, slot FROM chat LIMIT 1000"
df = tilt.dbQuery(query)
