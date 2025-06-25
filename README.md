Databricks had just released the Free Edition: https://www.databricks.com/learn/free-edition
Naturally I wanted to try it out, since it's a tool on which I'd made my first ever project, I'm rather sentimental about it :D

As I'm fond of the Space Industry, I decided to dive into the APIs provided by Nasa regarding Near-Earth Objects. To be more explicit: asteroids that get "close" to Earth.
For that I did a first pipeline on Databricks Free Edition:
1. I created a Python Notebook that extracts data from the APIs and loads it in a spark dataframe
2. the spark dataframe is then written to a table in a staging schema in the database provided by Databricks
3. using the power of ELT, I wrote queries to transform the data from the staging area ( a STG_NASA schema) in more useful tables that are stored in the Data Warehouse level (a DWH_NASA schema)
4. All of this runs on a daily schedule, directly set on Databricks

Apart from the nasa-related tables. I also created 2 technical tables in a TECH schema:
1. NASA_PARAM which has a parameter: EXECUTION_MODE with 2 possible values: FULL or DELTA. At each run, the script will fetch the parameter's value and during the first execution, the pipeline is run in FULL mode, and once it is finished successfully, it updates that table's parameter to DELTA mode in order for future runs to be executed incrementally
  that way, it is possible to force a full refresh by just updating manually that parameter to FULL.
  there also is a s_full_refresh_date_ts if we want to refresh from a specific date
![image](https://github.com/user-attachments/assets/ffe9294e-ee6e-43e4-a384-9edd6938f9b7)

2. RUN_NASA_MANAGEMENT which logs each run with the following values: run id, run name, run status, run message, run start date, run end date
![image](https://github.com/user-attachments/assets/23acda25-54b3-415e-bb44-bdac866e990f)


--------------------------------------------

Out of curiosity I also wanted to create the same pipeline but on my local machine using a Python script and load the data to a Postgresql Database
The main logic is the same, the technical tables are used the same way but the python script is slightly different since I did not use a spark dataframe anymore but instead used pandas, sqlalchemy to load to the Postgresql database
The really interesting part was to set the pipeline to be executed on a schedule set on PREFECT
So I had to configure the virtual environment to connect to a PREFECT's pool.

below is the general architecture
![nasa_hub](https://github.com/user-attachments/assets/980a0a2c-4878-4df1-b191-999c9ebc07ea)

The files are used for the local pipeline, but the python script for the Notebook on Databricks will be in the dedicated folder
