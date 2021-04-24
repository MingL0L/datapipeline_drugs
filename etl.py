import os
import json
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import countDistinct, collect_list, date_format, lower, col, to_date, when, col, lit

def create_spark_session():
    '''
    Create a new spark session
    
    Returns:
      spark(object): spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def stage_drug(spark, input_data, output_data, table):
    '''
    Processing drug files, load them into Spark session, clean and stage them
    into parquet

    Parameters:
        spark(object): spark session
        input_data(str): input file path
        output_data (str): output file path
    Returns:
    '''   
    print("stage_drug start")
    # load drugs
    df_drug = spark.read.options(header='True',inferSchema='True')\
                        .csv(input_data)
    # clean drugs
    df_drug = df_drug.withColumn('drug', lower(col('drug')))
   
    # Write to parquet
    df_drug.write.parquet(os.path.join(output_data, table), mode="overwrite")
    print("stage_drug end")

def stage_pubmed(spark, input_data, output_data, table):
    '''
    Processing stage_pubmed files, load them into Spark session, clean and stage them
    into parquet

    Parameters:
        spark(object): spark session
        input_data(str): input file path
        output_data (str): output file path
    Returns:
    '''   
    print("stage_pubmed start")
    #load pubmed file
    df_pubmed = spark.read.options(header='True',inferSchema='True')\
                        .csv(input_data)
    # clean pubmed
    df_pubmed = df_pubmed.withColumn('title', lower(col('title')))\
                        .withColumn("date", when(to_date('date', 'dd/MM/yyyy').isNotNull(),\
                                                 to_date('date', 'dd/MM/yyyy')).otherwise(col('date')))
   
    # Write to parquet
    df_pubmed.write.parquet(os.path.join(output_data, table), mode="overwrite")
    print("stage_pubmed end")
    
def stage_clinical_trials(spark, input_data, output_data, table):
    '''
    Processing clinical_trials files, load them into Spark session, clean and stage them
    into parquet

    Parameters:
        spark(object): spark session
        input_data(str): input file path
        output_data (str): output file path
    Returns:
    '''   
    print("stage_clinical_trials start")
    # load clinical_trials csv
    df_clinical = spark.read.options(header='True',inferSchema='True')\
                            .csv(input_data)
    
    # fill na and remove duplicated record
    w = Window.partitionBy(df_clinical.scientific_title, df_clinical.date)
    df_clinical = df_clinical.na.drop(subset=["scientific_title"])\
                .withColumn("journal", collect_list("journal").over(w)[0])\
                .withColumn("id", collect_list("id").over(w)[0])\
                .dropDuplicates()

    # change date format and lowercase title
    df_clinical = df_clinical.withColumn('scientific_title', lower(col('scientific_title')))\
                            .withColumnRenamed('scientific_title', 'title')\
                            .withColumn("date", when(to_date('date', 'dd MMMM yyyy').isNotNull(), \
                                                     to_date('date', 'dd MMMM yyyy')).otherwise(to_date('date', 'dd/MM/yyyy')))
   
    # Write to parquet
    df_clinical.write.parquet(os.path.join(output_data, table), mode="overwrite")
    print("stage_clinical_trials end")

def load_mention_records(spark, tbl_drug, tbl_pubmed, tbl_clinical, output_data, filename):
    '''
    Processing clinical_trials files, load them into Spark session, clean and stage them
    into parquet

    Parameters:
        spark(object): spark session
        input_data(str): input file path
        output_data (str): output file path
    Returns:
    '''   
    print("load_mention_records start")
    # load drug parquet
    df_drug = spark.read.parquet(os.path.join(output_data, tbl_drug))    
    # load pubmed parquet
    df_pubmed = spark.read.parquet(os.path.join(output_data, tbl_pubmed)) 
    # load clinical parquet
    df_clinical = spark.read.parquet(os.path.join(output_data, tbl_clinical))
    
    # Records drug name mentiond in pubmed
    df_drug_pubmed = df_drug.join(df_pubmed, \
                 df_pubmed.title.contains(df_drug.drug), \
                 how = "inner")\
                .withColumn('publication', lit('PubMed'))
    
    # Records drug name mentiond in clinical trials
    df_drug_clinical = df_drug.join(df_clinical, \
                 df_clinical.title.contains(df_drug.drug), \
                 how = "inner")\
                .withColumn('publication', lit('clinical trials'))
    
    # Union all the records
    mention_records = df_drug_pubmed.union(df_drug_clinical)
    
    # Save final dataframe into a single JSON file
    with open(os.path.join(output_data, filename),"w") as f:
        f.write(json.dumps(json.loads(mention_records.toPandas().to_json(orient='records')), indent=2))
    print("load_mention_records end") 

def check_count(spark, output_data, filename, description):
    '''
    Check number of rows, need to be greater than 0
    Input: Spark dataframe, description
    Output: Print out check result
    '''
    df = spark.read.option("multiLine","true").json(os.path.join(output_data, filename))
    cnt = df.count()
    if cnt == 0:
        raise ValueError('Quality check failed for {} with zero records!!'.format(description))
    else:
        print("Quality check succes for {} with {} records.".format(description,cnt))

def main():
    '''
    ETL pipeline process drugs, pubmed and clinical trials publications records in Spark 
    and load them into the final data model mehtion_records
    '''    
    spark = create_spark_session()
    # config
    output_data = "datalake"
    tbl_drug = "stage_drugs"
    tbl_pubmed = "stage_pubmed"
    tbl_clinical = "stage_clinical"
    file_final = "mention_records.json"
    
    # staging
    stage_drug(spark, "drugs.csv", output_data, tbl_drug)
    stage_pubmed(spark, "pubmed.csv", output_data, tbl_pubmed)
    stage_clinical_trials(spark, "clinical_trials.csv", output_data, tbl_clinical)
    
    # load final table
    load_mention_records(spark, tbl_drug, tbl_pubmed, tbl_clinical, output_data, file_final)
    
    # Quality check
    check_count(spark, output_data, file_final, "drugs mentions records")


if __name__ == "__main__":
    main()