package com.finalproject2.spark

import com.SparkSessionWrapper
import org.apache.spark.sql.SaveMode

object GenerateAvroFile extends SparkSessionWrapper{

  def main(args: Array[String]): Unit = {

    val user_dir = Seq((1, "Robert", "Smith"),
      (2, "John", "Johnson"),
      (3, "Alex", "Jones"),
      (4, "Jen", "Brown"),
      (5, "Michael", "Rose")
    )

    val message_dir = Seq((11, "RobertText"),
      (12, "JohnText"),
      (13, "AlexText"),
      (14, "JenText"),
      (15, "MichaelText")
    )

    val message = Seq((1, 11),
      (2, 12),
      (3, 13),
      (4, 14),
      (5, 15)
    )

    val retweet = Seq((1, 2, 11),
      (1, 3, 11),
      (1, 6, 11),
      (2, 5, 11),
      (3, 7, 11),
      (7, 14, 11),
      (5, 33, 11),
      (2, 4, 12),
      (3, 8, 13),
      (3, 15, 13),
      (15, 17, 13),
      (2, 11, 12),
      (3, 88, 13),
      (11, 22, 12)
    )

    val user_dirColumns = Seq("USER_ID", "FIRST_NAME", "LAST_NAME")
    val message_dirColumns = Seq("MESSAGE_ID", "TEXT")
    val messageColumns = Seq("USER_ID", "MESSAGE_ID")
    val retweetColumns = Seq("USER_ID", "SUBSCRIBER_ID", "MESSAGE_ID")

    import spark.implicits._
    val dfUser = user_dir.toDF(user_dirColumns: _*)
    val dfMessageDir = message_dir.toDF(message_dirColumns: _*)
    val dfMessage = message.toDF(messageColumns: _*)
    val dfRetweet = retweet.toDF(retweetColumns: _*)

    /**
     * Write Avro File
     */
    dfUser.write.format("avro")
      .mode(SaveMode.Overwrite)
      .save("/Users/pcichocki/Documents/GitHub/SPARK-Piotr-Cichocki/src/main/resources//USER_DIR.avro")

    dfMessageDir.write.format("avro")
      .mode(SaveMode.Overwrite)
      .save("/Users/pcichocki/Documents/GitHub/SPARK-Piotr-Cichocki/src/main/resources//MESSAGE_DIR.avro")

    dfMessage.write.format("avro")
      .mode(SaveMode.Overwrite)
      .save("/Users/pcichocki/Documents/GitHub/SPARK-Piotr-Cichocki/src/main/resources//MESSAGE.avro")

    dfRetweet.write.format("avro")
      .mode(SaveMode.Overwrite)
      .save("/Users/pcichocki/Documents/GitHub/SPARK-Piotr-Cichocki/src/main/resources/RETWEET.avro")

  }
}
