import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


trait SparkSessionTestWrapper {

  Logger.getLogger("org").setLevel(Level.ERROR)

  lazy val spark: SparkSession = {
    SparkSession
      .builder
      .master("local")
      .appName("spark test example")
      .getOrCreate
  }
}
