import sqlite3
from bs4 import BeautifulSoup
import requests
import time
import re

import pandas as pd
url = "https://www.data.jma.go.jp/stats/etrn/view/daily_a1.php?prec_no=48&block_no=1445&year=2020&month=11&day=&view=p1"
df = pd.read_html(url)[0]

df = df.drop(columns = ["湿度","雪","風向・風速"])
df.columns = ['date','rain_sum','rain_max_1h',"rain_max_10min","temperature_ave","temperature_max","temperature_min","sunlight"]

wether_list = []
wether_list2 = []
wether_list3 = []
wether_list4 = []
wether_list5 = []
wether_list6 = []
for index, row in df.iterrows():
    wether_list.append(row['date'])
    wether_list2.append(row["rain_sum"])
    wether_list3.append(row['temperature_ave'])
    wether_list4.append(row['temperature_max'])
    wether_list5.append(row['temperature_min'])
    wether_list6.append(row['sunlight'])

wether_all_list = list(zip(wether_list,wether_list2,wether_list3,wether_list4,wether_list5,wether_list6))
wether_all_list

# DBファイルを保存するためのファイルパス

# Google Colab
path = '/Users/iori/授業用/DSプログラム/github_dsprogram/'

# ローカル（自分のMac）
# path = '../db/'

# DBファイル名
db_name = 'weather_data2020.sqlite'

# DBに接続する（指定したDBファイル存在しない場合は，新規に作成される）
con = sqlite3.connect(path + db_name)

# DBへの接続を閉じる
con.close()


# 1．DBに接続する
con = sqlite3.connect(path + db_name)
# print(type(con))

# 2．SQLを実行するためのオブジェクトを取得
cur = con.cursor()

# 3．実行したいSQLを用意する
# テーブルを作成するSQL
# CREATE TABLE テーブル名（カラム名 型，...）;
sql_create_table_github = 'CREATE TABLE weather_data2020(date int, rain_sum int, avarage int, max int, min int, sunlight int);'

# 4．SQLを実行する
cur.execute(sql_create_table_github)

# 5．必要があればコミットする（データ変更等があった場合）
# 今回は必要なし

# 6．DBへの接続を閉じる
con.close()



# 1．DBに接続する
con = sqlite3.connect(path + db_name)
# print(type(con))

# 2．SQLを実行するためのオブジェクトを取得
cur = con.cursor()

# 3．SQLを用意
# データを挿入するSQL
# INSERT INTO テーブル名 VALUES (列に対応したデータをカンマ区切りで);
sql_insert_many = "INSERT INTO weather_data2020 VALUES (?, ?, ?, ?, ?, ?);"

# 4．SQLを実行
cur.executemany(sql_insert_many, wether_all_list)

# 5．コミット処理（データ操作を反映させる）
con.commit()

# 6．DBへの接続を閉じる
con.close()


# 1．DBに接続する
con = sqlite3.connect(path + db_name)
# print(type(con))

# 2．SQLを実行するためのオブジェクトを取得
cur = con.cursor()

# 3．SQLを用意
# SELECT * FROM テーブル名;
# *の部分は取得したい列の名前をカンマ区切りで指定することもできる
sql_select = 'SELECT * FROM weather_data2020;'

# 4．SQLを実行
cur.execute(sql_select)

for r in cur:
  print(r)

# 6．DBへの接続を閉じる
con.close()