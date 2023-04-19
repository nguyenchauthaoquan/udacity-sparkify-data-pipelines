# Project 5: Udacity Sparkify Data Pipelines
### Introduction
In this project, I will build the sparkify data pipelines
### Datasets
we'll be working with two datasets. Here are the s3 links provided by udacity for each:

Log data: s3://udacity-dend/log_data
Song data: s3://udacity-dend/song_data
### Database Design
``` plantuml
@startuml
entity "staging_events" as staging_events {
    artist: VARCHAR(255),
    auth: VARCHAR(255),
    firstName: VARCHAR(255),
    gender: CHAR(1),
    itemInSession: INTEGER,
    lastName: VARCHAR(255),
    length: FLOAT,
    level: VARCHAR(255),
    location: VARCHAR(255),
    method: VARCHAR(255),
    page: VARCHAR(255),
    registration: FLOAT,
    sessionId: INTEGER,
    song: VARCHAR(255),
    status: INTEGER,
    ts: TIMESTAMP,
    userAgent: VARCHAR(255),
    userId: INTEGER
}

entity "staging_songs" as staging_songs {
    artist_id: VARCHAR(255),
    artist_latitude: FLOAT,
    artist_location: VARCHAR(255),
    artist_longitude: FLOAT,
    artist_name: VARCHAR(255),
    duration: FLOAT,
    num_songs: INTEGER,
    song_id: VARCHAR(255),
    title: VARCHAR(255),
    year: INTEGER
}

entity "users" as users {
    user_id: INTEGER NOT NULL <<DISTKEY>> <<SORTKEY>> <<PK>>, 
    first_name: VARCHAR(255) NOT NULL,
    last_name: VARCHAR(255) NOT NULL, 
    gender: CHAR(1) NOT NULL, 
    level: VARCHAR(255) NOT NULL
}

entity "songs" as songs {
     song_id: VARCHAR(255) NOT NULL <<DISTKEY>> <<SORTKEY>> <<PK>>,
     title: VARCHAR(255) NOT NULL,
     artist_id: VARCHAR(255) NOT NULL,
     year: VARCHAR(255) NOT NULL,
     duration: FLOAT
}

entity "artists" as artists {
    artist_id: VARCHAR(255) NOT NULL <<DISTKEY>> <<SORTKEY>> <<PK>>,
    name: VARCHAR(255) NOT NULL,
    location: VARCHAR(255),
    latitude: FLOAT,
    longitude: FLOAT
}

entity "time" as time {
    start_time TIMESTAMP NOT NULL <<DISTKEY>> <<SORTKEY>> <<PK>>, 
    hour INTEGER NOT NULL, 
    day INTEGER NOT NULL, 
    week INTEGER NOT NULL, 
    month INTEGER NOT NULL, 
    year INTEGER NOT NULL, 
    weekday VARCHAR(20) NOT NULL
}

entity "songplays" as songplays {
    songplay_id: INTEGER <<auto incremental>> <<DISTKEY>> <<SORTKEY>> <<PK>>,
    start_time: TIMESTAMP NOT NULL <<SORTKEY>> <<FK>>,
    user_id: VARCHAR(255) NOT NULL <<SORTKEY>> <<FK>>,
    song_id: VARCHAR(255) <<SORTKEY>> <<FK>>,
    artist_id VARCHAR(255) <<SORTKEY>> <<FK>>,
    level VARCHAR(255),
    session_id INTEGER,
    location VARCHAR(255),
    user_agent VARCHAR(255)
}
@enduml 
```
#### Copy data from udacity-dend s3 to another s3
We can copy the data from udacity-dend to another s3
``` aws s3 cp s3://udacity-dend/log-data/ s3://quannct-sparkify/log-data/ --recursive ```
``` aws s3 cp s3://udacity-dend/song-data/ s3://quannct-sparkify/song-data/ --recursive ```
quannct-sparkify is my s3 buckets name, we can replace it to another name

### Directory description
``` opt/airflow/dags``` is the directory containing dags definition (this is from instruction https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
```opt/airflow/dags/sparkify.py``` is my dags of the project
```opt/airflow/logs ``` is the directory of airflow logs (this is from instruction https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
```opt/airflow/plugins``` is the directory of airflow plugins (this is from instruction https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
```opt/airflow/scripts ``` is the directory of sql scripts required to run dags
```opt/airflow/docker-compose.yaml``` is the priority setup airflow to run dags on local
```opt/airflow/.env```is the environment file to run ```docker compose up airflow-init``` and ```docker compose up```
According to the project templates downloaded from https://s3.amazonaws.com/video.udacity-data.com/topher/2019/February/5c6058dc_project-template/project-template.zip, it has error indicating Not found module, so I placed the operators in folder ```opt/airflow/dags/common/operators``` and helpers function is placed in ```opt/airflow/dags/common/helpers``` and import manually in ```sparkify.py```

### How to run airflow
#### Connection setup
- Create the IAM connection with connection id: aws_iam_credentials from Udacity instructions https://learn.udacity.com/nanodegrees/nd027/parts/cd12380/lessons/2985e8ce-7d29-49f5-ba79-fa31638d43dd/concepts/00c0b958-a65f-432c-9d20-120591bff571
- Create the redshift connection with connection id: from Udacity ínstruction https://learn.udacity.com/nanodegrees/nd027/parts/cd12380/lessons/2985e8ce-7d29-49f5-ba79-fa31638d43dd/concepts/fcd0636e-ca77-4c56-9357-b5a0bf9f4000
- Open cmd in opt folder
- Run ```docker compose up airflow-init``` for the first time
- Run ```docker compose up``` for the first time
- Access to airflow UI webserver and login account airflow and password airflow(can use another airflow account) 
- Unpause and trigger the dags whose name is sparkify-dag and run