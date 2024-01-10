import sqlite3
from bs4 import BeautifulSoup
import requests
import time
import re
import pandas as pd

df = pd.read_csv('sleep_infomation.csv')

df.columns = ['assessment','time_of_sleep','good_quality_sleep',"deep_sleep","heart_rate"]

sleep_list = []
sleep_list2 = []
sleep_list3 = []
sleep_list4 = []
sleep_list5 = []
for index, row in df.iterrows():
    sleep_list.append(row['assessment'])
    sleep_list2.append(row["time_of_sleep"])
    sleep_list3.append(row['good_quality_sleep'])
    sleep_list4.append(row['deep_sleep'])
    sleep_list5.append(row['heart_rate'])

sleep_all_list = list(zip(sleep_list,sleep_list2,sleep_list3,sleep_list4,sleep_list5))

path = '/Users/iori/授業用/DSプログラム/github_dsprogram/'

db_name = 'sleep_data2020.sqlite'

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
sql_create_table_github = 'CREATE TABLE sleep_data2020(assessment int, time_os_sleep int, good_quality_sleep int, deep_sleep int, heart_rate int);'

# 4．SQLを実行する
cur.execute(sql_create_table_github)

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
sql_insert_many = "INSERT INTO sleep_data2020 VALUES (?, ?, ?, ?, ?);"

# 4．SQLを実行
cur.executemany(sql_insert_many, sleep_all_list)

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
sql_select = 'SELECT * FROM sleep_data2020;'

# 4．SQLを実行
cur.execute(sql_select)

for r in cur:
  print(r)

# 6．DBへの接続を閉じる
con.close()