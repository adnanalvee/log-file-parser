package com.att.cdo.security.silvertail

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

import parserutils.Parser

object Main {
  val logger: Logger = Logger.getLogger(this.getClass)

  case class activityLog
    (
    timestamp: String, 
    field: String, 
    value: String
    )

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("SilverTailParser")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()

    // Required for converting to DataFrame
    import spark.implicits._

    val dataDir1 =
      "/Users/adnanalvee/Work/projects/security/projects/risk-engine/silvertail_sample_data/out-shard_7c/out-shard_7c-5.log"

    
    val rdd1 = 
    spark.sparkContext.textFile(dataDir1)
      .map(_.split("\t"))
      .filter(col => col(0).size > 0)
      .filter(col => col(0).substring(0, 1).toUpperCase != "T")
      .map(attributes =>
        activityLog(attributes(0), attributes(1), attributes(2)))
    logger.info("Data loaded and mapped successfully.")    


   val df1 = rdd1.toDF()

// replace "J" and "S" in column 'ip' with "D"
    val df2 = (df1
      .withColumn("timestamp", regexp_replace(col("timestamp"), "J", "D"))
      .withColumn("timestamp", regexp_replace(col("timestamp"), "S", "D")))

    val columns = List("field", "value")

// UDF to join two lists in order
    val zipper = udf[Seq[(String, String)], Seq[String], Seq[String]](_.zip(_))

// Grouping by timestamp and applying UDF to two sequences to join them in order.
    val df3 = (
      df2
        .groupBy("timestamp")
        .agg(
          collect_list(columns(0)) as "field",
          collect_list(columns(1)) as "values"
        )
        .withColumn(
          "field-value",
          zipper(col("field"), col("values"))
        )
      )
logger.info("UDF to join two lists in order executed successfully.")

// output schema
    val schema = StructType(
      df3.schema.fields ++ Array(
        StructField("method", StringType),
        StructField("page", StringType),
        StructField("host", StringType),
        StructField("jsession_id", StringType),
        StructField("atteshr", StringType),
        StructField("client_ip", StringType),
        StructField("client_port", StringType),
        StructField("server_ip", StringType),
        StructField("server_port", StringType),
        StructField("customer_email_address", StringType),
        StructField("om_req_id", StringType),
        StructField("account_number", StringType),
        StructField("notes", StringType),
        StructField("user_agent", StringType),
        StructField("user_agent_changed", StringType),
        StructField("sdata", StringType)
      ))

// Call function 'parseFieldValues'
    val rows = df3.rdd.map(
      r =>
        Row.fromSeq(
          r.toSeq ++ Parser.parseFieldValues(r.getAs("field-value"))
      ))
   
   logger.info("Values parsed successfully with new schema.")

    val df4 = spark.createDataFrame(rows, schema)

    val df5 = df4.drop("field", "values", "field-value")
    
  }
}
