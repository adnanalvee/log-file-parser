
package com.att.cdo.security.silvertail

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("SilverTailSpark")
      .getOrCreate()
  }

}