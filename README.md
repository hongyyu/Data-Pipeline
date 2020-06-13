## Introduction
Building a `Data Pipeline` and filling the `Data Warehouse` for the music streaming company Sparkify. 
To use `Apache Airflow` to build a data pipeline, we could monitor the whole processes during the ETL tasks, 
and data pipelines would become automotive and reusable. Meanwhile, it is easy to do the backfills and to 
check data quality with Airflow. The tasks of this project is to extract data from AWS S3 (format as `JSON`), 
and then transform and load data into staging tables and any other talbes designed at `Redshift Cluster`. 


## DAG (Directed Acyclic Graph)
Directed Asyslic Graph is a finited directed graph without directed cycles, and we should use a DAG to 
design our data pipeline. As the picture showing below, we design our data pipeline:


![dag_picture](/pictures/dag.png)

The steps are as follows:
1. **Create** all necessary tables at Redshift cluster
2. **Load** data from S3 into staging tables at Redshift
3. **Load** data from staging tables into fact and dimension tables
4. **Check** data quality of all fact and dimension tables

## Database Schema
Creating and designing the database with star schema. There are 5 tables in total (one fact
table and four dimension tables) showing below with column names in the second line:
#### *Fact Table*
   > songplays
   > - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
#### *Dimension Tables*
   > users
   > - user_id, first_name, last_name, gender, level

   > songs
   > - song_id, title, artist_id, year, duration

   > artists
   > - artist_id, name, location, lattitude, longitude

   > time
   > - start_time, hour, day, week, month, year, weekday
        
## How to Run
Instead of run Airflow in the local environment, it is easier to run with `docker` container. For installing 
docker, please click [here](https://docs.docker.com/docker-for-mac/install/). 
#### *Set up Docker container*
In order to set up and run Airflow using docker, after instlling docker in your local computer, 
there are several steps:

First, pull and download image to local 
```$xslt
docker pull puckel/docker-airflow
```
Then, run the docker image for Apache Airflow, and now you could visit Airflow UI at 
[http://localhost:8080/](http://localhost:8080/). For make your local project work in the docker container,
set appropriate path `/path/to/your/dag` below.
<pre>
docker run -d -p 8080:8080 -v <b>/path/to/your/dag</b>:/usr/local/airflow/dags  puckel/docker-airflow webserver
</pre>
It is also possible to use command line.
```$xslt
docker exec -ti <container name> bash
```
#### *Set up Connection to **AWS** and **Redshift***
On the Airflow UI, follow the tab Admin -> Connections -> Create to set proper connection to both `AWS` and 
`Redshift`.
- `AWS`
    - Conn Id: aws_credentials
    - Conn Type: Amazon Web Services
    - Login: **Your AWS ID**
    - Password: **Your AWS Secret Key**
- `Redshift`
    - Conn Id: redshift
    - Conn Type: Postgres
    - Host: **Your Redshift Cluster Endpoint**
    - Schema: **Database Name**
    - Login: **Redshift User Name**
    - Password: **Redshift User Password**
    - Port: **Redshift Port**
#### *Run the DAG*
1. `Open` the Airflow UI
2. `Turn On` the DAG so that the Scheduler could schedule what tasks to put into the queue.
3. Click `Trigger DAG` and then wait until complete.