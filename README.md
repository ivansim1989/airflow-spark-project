# airflow-spark-project

## _Desscription_
An e-commerce company requires that users sign up for a membership on the website in order to purchase a product from the platform. As a data engineer under this company, you are tasked with designing and implementing a pipeline to process the membership applications submitted by users on an hourly interval.

Applications are batched into a varying number of datasets and dropped into a folder on an hourly basis. You are required to set up a pipeline to ingest, clean, perform validity checks, and create membership IDs for successful applications. An application is successful if:

- Application mobile number is 8 digits
- Applicant is over 18 years old as of 1 Jan 2022
- Applicant has a valid email (email ends with @emailprovider.com or @emailprovider.net)

You are required to format datasets in the following manner:

- Split name into first_name and last_name
- Format birthday field into YYYYMMDD
- Remove any rows which do not have a name field (treat this as unsuccessful applications)
- Create a new field named above_18 based on the applicant's birthday
- Membership IDs for successful applications should be the user's last name, followed by a SHA256 hash of the applicant's birthday, truncated to first 5 digits of hash (i.e <last_name>_<hash(YYYYMMDD)>)

You are required to consolidate these datasets and output the successful applications into a folder, which will be picked up by downstream engineers. Unsuccessful applications should be condolidated and dropped into a separate folder.

You can use common scheduling solutions such as cron or airflow to implement the scheduling component. Please provide a markdown file as documentation.

Note: Please submit the processed dataset and scripts used


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