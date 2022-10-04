from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

import time
from datetime import datetime
import csv
import requests
import json
import numpy as np
import pandas as pd
import psycopg2
import warnings
from pandas.core.common import SettingWithCopyWarning

# 기본적인 정보 
args = {'owner': 'airflow', 'start_date': datetime(2022,8,31)}

dag  = DAG(dag_id='lolfinal',
           default_args=args,
)

# lol Match_id를 저장할 table 생성하기
create_matchID_sql_query = """
CREATE TABLE IF NOT EXISTS my_match1 (
    match_id VARCHAR(64) PRIMARY KEY
);
"""

def _puuid(): #puuid 불러오기
    summoner_Name = 'one summer drive' #소환사명
    api_key = "RGAPI-e9182012-d083-4dde-a741-cac974a80f5c" # api key
    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://developer.riotgames.com",
        "X-Riot-Token": "RGAPI-e9182012-d083-4dde-a741-cac974a80f5c"
    }
    summoner_url = "https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/"+summoner_Name+"/""?api_key="+api_key
    return requests.get(summoner_url, headers=request_headers).json()['puuid']

def _summoner_match(): # puuid를 통해서 match정보 불러오기
    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://developer.riotgames.com",
        "X-Riot-Token": "RGAPI-e9182012-d083-4dde-a741-cac974a80f5c"
    }
    summoner_puuid = _puuid()
    match_url = "https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/"+summoner_puuid+"/ids?start=0&count=10"
    return requests.get(match_url, headers=request_headers).json()

def _insert_db_matchID(): #최근게임 10경기를 불러오고 postgresql에 저장하기 100경기로 할 경우 API호출 문제 발생
    match10 = _summoner_match()
    hook = PostgresHook(postgres_conn_id = 'postgres_local')
    conn = hook.get_conn()
    cur = conn.cursor()
    for m in match10:
        cur.execute("INSERT INTO my_match(match_id) VALUES(%s) ON CONFLICT (match_id) DO NOTHING", [m])
    conn.commit()

create_game_match_sql_query = """
CREATE TABLE IF NOT EXISTS game1 (
    match_id VARCHAR(64) REFERENCES my_match (match_id),
    puuid VARCHAR(255) NOT NULL,
    teamPosition VARCHAR(255),
    individualPosition VARCHAR(255),
    champName VARCHAR(255),
    champExp INTEGER,
    summoner_spell1 INTEGER,
    summoner_spell2 INTEGER,
    kills INTEGER,
    deaths INTEGER,
    assists INTEGER,
    damageDealtToBuildings INTEGER,
    totalDamageDealtToChamp INTEGER,
    damagePerMin NUMERIC(10,3),
    teamDamagePer INTEGER,
    killParticipationPer NUMERIC (10,3),
    totalDamageTaken INTEGER,
    damageTakeOnTeamPer INTEGER,
    goldEarned INTEGER, 
    goldPerMin NUMERIC (10,3),
    totalCs INTEGER,
    maxCsAdvantageOnLaneOpponent NUMERIC (10,3),
    maxLevelLeadLaneOpponent INTEGER,
    gameEndedInEarlySurren BOOLEAN,
    gameEndedInSurren BOOLEAN,
    teamEarlySurren BOOLEAN,
    win BOOLEAN,
    timePlayed NUMERIC (10,3),
    unique (match_id, puuid)
);
"""

def game_info(match_id_num): #match_id_num
    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://developer.riotgames.com",
        "X-Riot-Token": "RGAPI-e9182012-d083-4dde-a741-cac974a80f5c"
    }
    game_url = "https://asia.api.riotgames.com/lol/match/v5/matches/"+match_id_num
    return requests.get(game_url, headers=request_headers).json()['info']['participants']

def _game():
    game_df = pd.DataFrame(columns=['match','puuid', 'teamPosition', 'individualPosition', 'champName', 'champExp', 'summoner_spell1', 'summoner_spell2' ,
                                'kills', 'deaths', 'assists', 'damageDealtToBuildings', 'totalDamageDealtToChamp', 'damagePerMin', 'teamDamagePer(%)',
                                'killParticipation(%)', 'totalDamageTaken', 'damageTakeOnTeamPer(%)', 'goldEarned', 'goldPerMin', 'totalCs',
                                'maxCsAdvantageOnLaneOpponent', 'maxLevelLeadLaneOpponent', 'gameEndedInEarlySurren', 'gameEndedInSurren',
                                'teamEarlySurrn', 'win', 'timePlayed'])
    match10 = _summoner_match()
    for i in range(len(match10)):
        print(i)
        game_content = game_info(match10[i])
        for j in range(len(game_content)):
            
            try:
                input_data = {
                    'match':match10[i],
                    'puuid':game_content[j]['puuid'], 
                    'teamPosition':game_content[j]['teamPosition'], 
                    'individualPosition':game_content[j]['individualPosition'], 
                    'champName':game_content[j]['championName'], 
                    'champExp':float(game_content[j]['champExperience']), 
                    'summoner_spell1':float(game_content[j]['summoner1Id']), 
                    'summoner_spell2':float(game_content[j]['summoner2Id']),
                    'kills':float(game_content[j]['kills']), 
                    'deaths':float(game_content[j]['deaths']), 
                    'assists':float(game_content[j]['assists']), 
                    'damageDealtToBuildings':float(game_content[j]['damageDealtToBuildings']), 
                    'totalDamageDealtToChamp':float(game_content[j]['totalDamageDealtToChampions']),
                    'damagePerMin':game_content[j]['challenges']['damagePerMinute'], 
                    'teamDamagePer(%)':round(game_content[j]['challenges']['teamDamagePercentage'], 2)*100,
                    'killParticipation(%)':game_content[j]['challenges']['killParticipation']*100,
                    'totalDamageTaken':float(game_content[j]['totalDamageTaken']),
                    'damageTakeOnTeamPer(%)':round(game_content[j]['challenges']['damageTakenOnTeamPercentage'],2)*100,
                    'goldEarned':float(game_content[j]['goldEarned']),
                    'goldPerMin':game_content[j]['challenges']['goldPerMinute'],
                    'totalCs':float(game_content[j]['totalMinionsKilled']+game_content[j]['neutralMinionsKilled']),
                    'maxCsAdvantageOnLaneOpponent':float(game_content[j]['challenges']['maxCsAdvantageOnLaneOpponent']),
                    'maxLevelLeadLaneOpponent':float(game_content[j]['challenges']['maxLevelLeadLaneOpponent']),
                    'gameEndedInEarlySurren':game_content[j]['gameEndedInEarlySurrender'],
                    'gameEndedInSurren':game_content[j]['gameEndedInSurrender'],
                    'teamEarlySurrn':game_content[j]['teamEarlySurrendered'],
                    'win':game_content[j]['win'],
                    'timePlayed':float(round(game_content[j]['timePlayed']/60,2)*100) # 4자리로 앞의 2자리는 분, 뒤의 2자리는 초로 구성되어있다. 
                    }
                game_df = game_df.append(input_data, ignore_index=True)
            except:
                input_data = {
                    'match':match10[i],
                    'puuid':game_content[j]['puuid'], 
                    'teamPosition':game_content[j]['teamPosition'], 
                    'individualPosition':game_content[j]['individualPosition'], 
                    'champName':game_content[j]['championName'], 
                    'champExp':float(game_content[j]['champExperience']), 
                    'summoner_spell1':float(game_content[j]['summoner1Id']), 
                    'summoner_spell2':float(game_content[j]['summoner2Id']),
                    'kills':float(game_content[j]['kills']), 
                    'deaths':float(game_content[j]['deaths']), 
                    'assists':float(game_content[j]['assists']), 
                    'damageDealtToBuildings':float(game_content[j]['damageDealtToBuildings']), 
                    'totalDamageDealtToChamp':float(game_content[j]['totalDamageDealtToChampions']),
                    'damagePerMin':game_content[j]['challenges']['damagePerMinute'], 
                    'teamDamagePer(%)':round(game_content[j]['challenges']['teamDamagePercentage'], 2)*100,
                    'killParticipation(%)':0,
                    'totalDamageTaken':float(game_content[j]['totalDamageTaken']),
                    'damageTakeOnTeamPer(%)':round(game_content[j]['challenges']['damageTakenOnTeamPercentage'],2)*100,
                    'goldEarned':float(game_content[j]['goldEarned']),
                    'goldPerMin':game_content[j]['challenges']['goldPerMinute'],
                    'totalCs':float(game_content[j]['totalMinionsKilled']+game_content[j]['neutralMinionsKilled']),
                    'maxCsAdvantageOnLaneOpponent':0,
                    'maxLevelLeadLaneOpponent':0,
                    'gameEndedInEarlySurren':game_content[j]['gameEndedInEarlySurrender'],
                    'gameEndedInSurren':game_content[j]['gameEndedInSurrender'],
                    'teamEarlySurrn':game_content[j]['teamEarlySurrendered'],
                    'win':game_content[j]['win'],
                    'timePlayed':float(round(game_content[j]['timePlayed']/60,2)*100) # 4자리로 앞의 2자리는 분, 뒤의 2자리는 초로 구성되어있다. 
                    }
                time.sleep(1)
    game_df.to_csv('gamedata.csv', header=False, index=False)
    return game_df

def _insert_db_game(): #진행한 게임에 대한 정보를 Postgres에 저장하기
    game10 = _game()
    print(game10.loc[1])
    hook = PostgresHook(postgres_conn_id = 'postgres_local')
    conn = hook.get_conn()
    cur = conn.cursor()
    for g in range(len(game10)):
        cur.execute("INSERT INTO game VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (match_id, puuid) DO NOTHING", game10.loc[g])
    conn.commit()

create_table_match = PostgresOperator(
    sql = create_matchID_sql_query,
    task_id = 'create_table_matchID',
    postgres_conn_id = 'postgres_local',
    dag = dag
)

insert_match_data = PythonOperator(
    task_id = 'save_my_match',
    python_callable=_insert_db_matchID,
    dag=dag
)

create_table_game = PostgresOperator(
    sql = create_game_match_sql_query,
    task_id = 'create_table_game',
    postgres_conn_id='postgres_local',
    dag=dag
)

insert_game_data = PythonOperator(
    task_id = 'save_my_game',
    python_callable=_insert_db_game,
    dag=dag
)

postgres_to_gcs_task = PostgresToGCSOperator(
    task_id = 'postgres_to_gcs_task',
    postgres_conn_id='postgres_local', #Airflow Webserver내 작성한 Postgres Connection
    sql = 'SELECT * FROM game;', # Postgresql에서 하고자 하는 SQL문
    bucket= 'lol-data', # GCP bucket 이름
    filename= 'game/mygame.csv', # 만들고자 하는 폴더/파일명
    export_format= 'csv',
    dag=dag
)

gcs_to_bigquery = GCSToBigQueryOperator(
    task_id = 'gcs_to_bigquery_task',
    gcp_conn_id='google_cloud_default', #Airflow Webserver Connection
    bucket= 'lol-data', # bucket-name
    source_objects=['game/mygame.csv'], # GCS File name
    destination_project_dataset_table='game.game', # Dataset.tablename
    autodetect=True, # Schema 지정없이 자동으로 컬럼명, 데이터 타입 지정
    source_format='csv', #데이터 형태
    write_disposition='WRITE_TRUNCATE', # 만약 테이블이 이미 존재하다면 데이터를 덮어쓴다.
    skip_leading_rows=1, # 1번째 행을 제외하고 만들기 (1번쨰 행은 Default)
    dag=dag
)

puuid = _puuid()
query = f"""
        SELECT *
        FROM game.game
        WHERE puuid = '{puuid}'
"""

bigquery_task = BigQueryOperator(
    task_id = 'bigquery_myinfo_task',
    gcp_conn_id='google_cloud_default',
    sql=query,
    destination_dataset_table = 'airflow-project-362006.game.my-game',
    write_disposition='WRITE_TRUNCATE',
    dag=dag

)

#bigquery_task_

create_table_match >> [insert_match_data, create_table_game] >> insert_game_data >> postgres_to_gcs_task >> gcs_to_bigquery >> bigquery_task