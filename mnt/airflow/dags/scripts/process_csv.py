import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, concat, lit
from pyspark.sql.types import StringType, StructType, StructField
from dateutil.parser import parse
import hashlib
from setup_logger import setup_logging

warehouse_location = os.path.abspath('spark-warehouse')

def main(datasets):
    # Setup spark session
    spark_conf = SparkConf().setAppName("Data Pipelines") \
        .setMaster('local[*]') \
        .set("spark.hadoop.fs.defaultFS", "file:///")

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .getOrCreate()

    current_path = '/opt/airflow'

    for dataset in datasets:
        try:
            # Read CSV from parent path
            df = spark.read.csv(f'{current_path}/{dataset}', header=True)

            # Standardize date format for DOB
            standardize_date_udf = udf(standardize_date, StringType())
            df = df.withColumn('date_of_birth', standardize_date_udf(col('date_of_birth')))

            # Remove all prefixes and suffixes
            remove_prefix_suffix_udf = udf(remove_prefix_suffix, StringType())
            df = df.withColumn('name', remove_prefix_suffix_udf(col('name')))

            # Split name to first and last name
            schema = StructType([
                StructField('first_name', StringType(), True),
                StructField('last_name', StringType(), True)
            ])

            split_name_udf = udf(split_name, schema)

            df = df.withColumn('split_name', split_name_udf(col('name'))) \
                .withColumn('first_name', col('split_name.first_name')) \
                .withColumn('last_name', col('split_name.last_name')) \
                .drop('split_name')

            # Reorganize the dataframe
            df = df.select('first_name', 'last_name', 'email', 'date_of_birth', 'mobile_no')

            # Filter successful application
            age_cutoff = 20040101  # 18 year old cutoff on 2022-01-01
            is_over_18 = col('date_of_birth').cast('integer') <= age_cutoff
            has_valid_email = col('email').rlike(r'^.+@.+(\.com|\.net)$')
            has_valid_mobile_no = col('mobile_no').rlike(r'^\d{8}$')
            has_name = col('first_name').isNotNull() & col('last_name').isNotNull()

            successful_df = df.filter(is_over_18 & has_valid_email & has_valid_mobile_no & has_name)

            # Assign Membership IDs for successful applications with required format
            hash_birthdate_udf = udf(hash_birthdate, StringType())
            successful_df = successful_df.withColumn(
                "membership_id",
                concat(col("last_name"), lit("_"), hash_birthdate_udf(col("date_of_birth")))
            )

            # Filter unsuccessful application using email
            unsuccessful_df = df.join(successful_df, ['email'], 'left_anti')

            # Write results to respective paths
            write_output_files(successful_df, unsuccessful_df, current_path, dataset)
            logger.info('Job Done')
        except Exception as error:
            logger.error('Error Message: %s', error)

def standardize_date(date_str):
    """
    Standardize date format to YYYYMMDD
    """
    try:
        date = parse(date_str)
        return date.strftime('%Y%m%d')
    except ValueError:
        return None

def remove_prefix_suffix(name):
    """
    Remove all prefixes and suffixes by pattern
    """
    words = name.split()

    # Remove prefix if the first word ends with a dot
    if words[0][-1] == '.':
        words.pop(0)

    # Remove suffix if the last word is upper case or ends with a dot
    if words[-1][-1].isupper() or words[-1][-1] == '.':
        words.pop()

    # Join the words back together and return the cleaned name
    return ' '.join(words)

def split_name(name):
    """
    Split name to first and last name
    """
    parts = name.split()
    # All name before last name is first name
    first_names = parts[:-1]
    first_name = ' '.join(first_names)
    # Last name
    last_name = parts[-1]
    return {'first_name': first_name, 'last_name': last_name}

def hash_birthdate(date_of_birth):
    """
    Return first 5 characters of birthdate hash
    """
    # Create a SHA256 hash object
    hash_object = hashlib.sha256()

    # Update the hash object with the bytes of the input string
    hash_object.update(date_of_birth.encode('utf-8'))

    # Get the hexadecimal digest of the hash
    birth_date_hex = hash_object.hexdigest()

    # truncated to first 5 digits of hash
    return birth_date_hex[:5]

def write_output_files(successful_df, unsuccessful_df, current_path, dataset):
    """
    Write results to respective paths
    """    
    output_dirs = {
        'successful_applications': successful_df,
        'unsuccessful_applications': unsuccessful_df
    }

    dataset = dataset.split(".")[0]

    for output_dir, output_df in output_dirs.items():
        output_path = f'{current_path}/{output_dir}'

        # create directory if not exist
        os.makedirs(os.path.join(output_path, dataset), exist_ok=True)

        # write to respective path
        output_df.write.csv(f'{output_path}/{dataset}', mode='overwrite', header=True)
    
if __name__ == "__main__":
    logger = setup_logging()
    import sys
    datasets = sys.argv[1:]
    main(datasets)