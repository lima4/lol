# LoL Data Analysis 

## Skill-Set 
* Docker 
  * [Airflow Docker Download](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
  * Docker-compose를 통하여 Airflow와 Postgresql을 설치하였습니다. 
  * 공식 홈페이지의 문서를 통해 진행하였습니다.
* Airflow
  * [Airflow Postgres & GCP Connection](https://www.notion.so/Airflow-bcf170d788304524a0fe4ba9067355a2)
  * [Airflow Document](https://airflow.apache.org/docs/)
  * Airflow Webserver의 Connection을 작성하여 Local의 Postgresql과 GCP를 Airflow와 연동하였습니다. 
  * 많은 정보가 없어 Stackoverflow를 참조하고 직접 질문을 하면서 작성하였습니다.
  * 기본적으로 Airflow Document를 가장 많이 참조하였습니다.
* Python
  * [LoL API](https://developer.riotgames.com/)
  * LoL Developer 홈페이지의 API Key를 사용하여 플레이한 게임 정보를 받아왔습니다.
  * LoL 서버의 문제로 인해서 다량의 데이터를 불러오지는 못하였습니다. 
* Linux
  * Window 환경에서 진행하여 Linux를 사용하여 Airflow 및 Dags를 작성하였습니다.
* PostgreSQL
  * LoL API에서 받아온 정보를 Postgresql에 테이블을 생성하고 저장하였습니다.
  * Match_ID를 저장하는 My_match 테이블과 My_match 테이블의 Match_ID를 참조하여 게임 정보를 저장하는 28개의 컬럼으로 이루어진 game테이블에 저장하였습니다. 
* GCP 
  * Local Postgresql의 데이터를 Airflow를 사용하여 GCS Bucket에 저장하였습니다. 
  * GCS Bucket에 저장된 데이터를 AIrflow의 Query문을 통해 Query문에 맞는 정보로 Bigquery에서 테이블을 생성하였습니다.
  * 지속적으로 데이터들이 추가되도록 작성하였습니다.  
* Tableau
  * Bigqueyr와 Tableau를 연동하였습니다.
