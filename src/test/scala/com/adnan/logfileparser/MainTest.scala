package com.adnan.logfileparser

import com.adnan.logfileparser.SparkSessionWrapper
import com.adnan.logfileparser.parserutils.Parser
import org.apache.log4j.{Level, Logger}

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

import org.scalatest._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.github.mrpowers.spark.fast.tests.DataFrameComparer

class Main extends FlatSpec with SparkSessionWrapper with DataFrameComparer {

  /*
   * Data is not real, complicated strings avoided
   * To test complicated string parse, check functions in 'ParserTest'
   */
  it should "correctly parse values according to schema provided" in {

    // To convert RDD to DF
    import spark.implicits._

    // Turn off Spark's Internal Logger
    Logger.getLogger("org").setLevel(Level.OFF)

    // Note: _om_req_id must be in required format before parsing or else its always an empty string output.
    val df1 = spark.sparkContext
      .parallelize(
        Seq(
          ("1", "REQUEST", "method=POST&page=bigcompany.com"),
          ("1", "HEADERS", "host=opus&dhost=kjl&user-agent=ipad"),
          ("1", "BARS", "JSESSIONID=XyMsdsd!dshtml&eshr=12345"),
          ("1", "TCP", "clientIp=1&serverIp=2&clientPort=3&serverPort=4"),
          ("1",
           "PARAMS",
           "accountNumber=123&_om_req_id=&emailId=example@bigcompany.com&notes=fraud"),
          ("1", "DISCO", "lots_of_stuff"),
          ("1", "AGENT", "windows"),
          ("2", "REQUEST", "Adnan"),
          ("2", "BARS", "Alvee")
        ))

    val df2 = df1.toDF("timestamp", "field", "value")

    val columns = List("field", "value")

    // UDF to join two lists in order
    val zipper = udf[Seq[(String, String)], Seq[String], Seq[String]](_.zip(_))

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

    val schema = StructType(
      df3.schema.fields ++ Array(
        StructField("method", StringType),
        StructField("page", StringType),
        StructField("host", StringType),
        StructField("jsession_id", StringType),
        StructField("eshr", StringType),
        StructField("client_ip", StringType),
        StructField("client_port", StringType),
        StructField("server_ip", StringType),
        StructField("server_port", StringType),
        StructField("email_address", StringType),
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

    val df4 = spark.createDataFrame(rows, schema)

    df4.show

    val actualDF = df4.drop("field", "values", "field-value")

    actualDF.show

    val expectedData = List(
      Row("1",
          "POST",
          "bigcompany.com",
          "opus",
          "XyMsdsd!dshtml",
          "12345",
          "1",
          "2",
          "3",
          "4",
          "example@bigcompany.com",
          "",
          "123",
          "fraud",
          "ipad",
          "windows",
          ""
          ),
      Row("2", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
    )


    val expectedSchema = Array(
      StructField("timestamp", StringType),
        StructField("method", StringType),
        StructField("page", StringType),
        StructField("host", StringType),
        StructField("jsession_id", StringType),
        StructField("eshr", StringType),
        StructField("client_ip", StringType),
        StructField("client_port", StringType),
        StructField("server_ip", StringType),
        StructField("server_port", StringType),
        StructField("email_address", StringType),
        StructField("om_req_id", StringType),
        StructField("account_number", StringType),
        StructField("notes", StringType),
        StructField("user_agent", StringType),
        StructField("user_agent_changed", StringType),
        StructField("sdata", StringType)
    )

    
    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    println("expected")
    expectedDF.show

    assertSmallDataFrameEquality(actualDF, expectedDF)

  }

}
