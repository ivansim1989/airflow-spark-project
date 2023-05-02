# airflow-spark-project

## _Instruction_

1. Open the parent directory of the project in VS code or your preferred code editors and open the **terminal**.
2. Execute **. start.sh** in the terminal to setup the docker environments.
3. Setup email alart
   i. Setup app passwords in https://security.google.com/settings/security/apppasswords
   ii. Setup smtp under **airflow.cfg** as below (The rests remind the same)
       smtp_host = smtp.gmail.com
       smtp_user = {your email}
       smtp_password = {password generated from app password}
       smtp_port = 587
       smtp_mail_from = {your email}
4. Open web browser and go to **http://localhost:8080/**
   user: airflow
   password: airflow
5. Setup spark connection
   i. Click on Admin on the top bar
   ii. Click on Connection and add a new connection name **spark_conn**
   iii. Change the conn Type to **Spark**
   iv. Input Host as **spark://spark-master**
   v. Input Port as **7077**
   vi. Save it
5. Switch on the DAG **process_csv** and click on it.
6. Click the play buttom on the top right for manual triggers (else it will be shceduled at 12am UTC daily)
7. Close the project by executing **. stop.sh** in terminal

**Note:** Assuming that you have already set up a basic development environment and docker desktop on your workstation.