package com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark: SparkSession = {
    SparkSession
      .builder
      .master("local")
      .appName("SparkSession")
      .getOrCreate
  }
}
