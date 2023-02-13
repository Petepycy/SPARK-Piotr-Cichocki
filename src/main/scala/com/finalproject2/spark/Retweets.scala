package com.finalproject2.spark

import com.SparkSessionWrapper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Retweets extends SparkSessionWrapper{

  def loadAvro(path: String): DataFrame = {
    spark.read.format("avro").load(path)
  }

  def nWaveJoin(message: DataFrame, retweet: DataFrame): DataFrame = {
    val firstWaveJoin = message.join(retweet, Seq("USER_ID", "MESSAGE_ID"))
      .withColumnRenamed("SUBSCRIBER_ID", "SUBSCRIBER_WAVE1_ID")
      .withColumnRenamed("MESSAGE_ID", "MESSAGE_WAVE1_ID")
      .withColumnRenamed("USER_ID", "USER_WAVE1_ID")
    firstWaveJoin
  }

  def waveCounter(nWaveJoin: DataFrame, groupByName: String, name: String): DataFrame = {
    val nWaveCount = nWaveJoin
      .groupBy(groupByName)
      .count()
      .withColumnRenamed("count", name)
    nWaveCount
  }

  def sumOfRetweets(joinOfRetweets: DataFrame): DataFrame = {
    val sumOfRetweets = joinOfRetweets
      .select(col("USER_WAVE1_ID1").as("USER_ID"), (col("Wave_1_count") + col("Wave_2_count"))
        .alias("NUMBER_RETWEETS"))
    sumOfRetweets
  }
  def main(args: Array[String]): Unit = {

    val user_dir = loadAvro("src/main/resources/USER_DIR.avro")

    val message_dir = loadAvro("src/main/resources/MESSAGE_DIR.avro")

    val message = loadAvro("src/main/resources/MESSAGE.avro")

    val retweet = loadAvro("src/main/resources/RETWEET.avro")

    //println("Wave 1")
    val firstWaveJoin = nWaveJoin(message, retweet)

    //println("Wave 1 count")
    val firstWaveCount = waveCounter(firstWaveJoin, "USER_WAVE1_ID", "Wave_1_count")

    //println("Wave 2")
    val secondWaveJoin = firstWaveJoin
      .join(retweet,
        firstWaveJoin("SUBSCRIBER_WAVE1_ID") === retweet("USER_ID")
          && firstWaveJoin("MESSAGE_WAVE1_ID") === retweet("MESSAGE_ID"))
      .withColumnRenamed("SUBSCRIBER_ID", "SUBSCRIBER_WAVE2_ID")
      .withColumnRenamed("MESSAGE_ID", "MESSAGE_WAVE2_ID")
      .withColumnRenamed("USER_ID", "USER_WAVE2_ID")

    //println("Wave 2 count")
    val secondWaveCount = waveCounter(secondWaveJoin, "USER_WAVE1_ID", "Wave_2_count")

//    println("Wave 3")
//    val thirdWaveJoin = secondWaveJoin
//      .join(retweet, secondWaveJoin("SUBSCRIBER_WAVE2_ID") === retweet("USER_ID")
//      && secondWaveJoin("MESSAGE_WAVE2_ID") === retweet("MESSAGE_ID"))
//      .withColumnRenamed("SUBSCRIBER_ID", "SUBSCRIBER_WAVE3_ID")
//      .withColumnRenamed("MESSAGE_ID", "MESSAGE_WAVE3_ID")
//      .withColumnRenamed("USER_ID", "USER_WAVE3_ID")
//
//    //thirdWaveJoin.show()
//
//    val toShow3 = thirdWaveJoin
//      .select("USER_WAVE1_ID", "USER_WAVE2_ID", "SUBSCRIBER_WAVE2_ID", "SUBSCRIBER_WAVE3_ID").show()
//
//    println("Wave 3 count")
//    val thirdWaveCount = thirdWaveJoin
//      .groupBy("USER_WAVE1_ID")
//      .count()
//      .withColumnRenamed("count", "Wave_3_count")
//
//    thirdWaveCount.show()

    val afterRename = firstWaveCount.withColumnRenamed("USER_WAVE1_ID", "USER_WAVE1_ID1")
    val joinOfRetweets1And2 = afterRename
      .join(secondWaveCount, afterRename("USER_WAVE1_ID1") === secondWaveCount("USER_WAVE1_ID"), "fullouter")
      .withColumnRenamed("USER_WAVE1_ID", "USER_WAVE1_ID2")
      .na.fill(0)

    //joinOfRetweets1And2.show()

//    val joinOfRetweets1And2And3 = joinOfRetweets1And2
//      .join(thirdWaveCount, joinOfRetweets1And2("USER_WAVE1_ID2") === thirdWaveCount("USER_WAVE1_ID"), "fullouter")
//      .withColumnRenamed("USER_WAVE1_ID", "USER_WAVE1_ID3")
//      .select("USER_WAVE1_ID1", "Wave_1_count", "Wave_2_count", "Wave_3_count")
//      .na.fill(0)

    //joinOfRetweets1And2And3.show()
//    // with 3 waves
//    println("Number of retweets")
//    val sumOfRetweets = joinOfRetweets1And2And3
//      .select(col("USER_WAVE1_ID1").as("USER_ID"),(col("Wave_1_count") + col("Wave_2_count") + col("Wave_3_count"))
//      .alias("NUMBER_RETWEETS"))

    println("Number of retweets")
    val sumOfRetweet = sumOfRetweets(joinOfRetweets1And2)
    sumOfRetweet.show()

    println("\"Top ten user\"")
    val joinResult = user_dir
      .join(message, usingColumn = "USER_ID")
      .join(message_dir, usingColumn = "MESSAGE_ID")

    val result = joinResult
      .select("USER_ID", "FIRST_NAME", "LAST_NAME", "MESSAGE_ID", "TEXT")
      .join(sumOfRetweet, "USER_ID")
      .sort(col("NUMBER_RETWEETS").desc)

    result.show(10)
  }
}
