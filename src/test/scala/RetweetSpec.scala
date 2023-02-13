import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.scalatest.funspec.AnyFunSpec
import com.finalproject2.spark.Retweets

class RetweetSpec extends AnyFunSpec with SparkSessionTestWrapper with DatasetComparer {

  import spark.implicits._

  val user_dir: DataFrame = Retweets.loadAvro("src/main/resources/USER_DIR.avro")
  val message_dir: DataFrame = Retweets.loadAvro("src/main/resources/MESSAGE_DIR.avro")
  val message: DataFrame = Retweets.loadAvro("src/main/resources/MESSAGE.avro")
  val retweet: DataFrame = Retweets.loadAvro("src/main/resources/RETWEET.avro")

  it("avro file load") {

    val actualUser_dir = user_dir.select(col("FIRST_NAME").alias("Name"))
    val actualMessage_dir = message_dir.select(col("TEXT").alias("Text"))
    val actualMessage = message.select(col("USER_ID").alias("ID"))
    val actualRetweet = retweet.select(col("SUBSCRIBER_ID").alias("Sub"))


    val expectedUser_dir = Seq(
      "Jen",
      "Michael",
      "John",
      "Robert",
      "Alex"
    ).toDF("Name")

    val expectedMessage_dir = Seq(
      "JenText",
      "MichaelText",
      "RobertText",
      "AlexText",
      "JohnText"
    ).toDF("Text")

    val expectedMessage = Seq(
      5,
      4,
      2,
      3,
      1
    ).toDF("ID")

    val expectedRetweet = Seq(
      17,
      11,
      88,
      22,
      5,
      7,
      14,
      33,
      2,
      3,
      6,
      4,
      8,
      15
    ).toDF("Sub")

    assertSmallDatasetEquality(actualUser_dir, expectedUser_dir, orderedComparison = false)
    assertSmallDatasetEquality(actualMessage_dir, expectedMessage_dir, orderedComparison = false)
    assertSmallDatasetEquality(actualMessage, expectedMessage, ignoreNullable = true, orderedComparison = false)
    assertSmallDatasetEquality(actualRetweet, expectedRetweet, ignoreNullable = true, orderedComparison = false)

  }

  it("wave count") {
    val firstWaveJoin = Retweets.nWaveJoin(message, retweet)
    val actualFirstWaveCount = Retweets.waveCounter(firstWaveJoin, "USER_WAVE1_ID", "Wave_1_count")
    val result = actualFirstWaveCount.select(col("USER_WAVE1_ID"), col("Wave_1_count").cast(IntegerType))
    val expectedFirstWaveCount = Seq(
      Row(1, 3),
      Row(3, 3),
      Row(2, 2)
    )
    val expectedSchema = StructType(Array(
      StructField("USER_WAVE1_ID", IntegerType),
      StructField("Wave_1_count", IntegerType)
    ))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedFirstWaveCount), StructType(expectedSchema))
    assertSmallDatasetEquality(result, df, ignoreNullable = true)
  }

  it("retweets count in wave 1 and 2") {
    val firstWaveJoin = Retweets.nWaveJoin(message, retweet)
    val actualFirstWaveCount = Retweets.waveCounter(firstWaveJoin, "USER_WAVE1_ID", "Wave_1_count")
    val result = actualFirstWaveCount.select(col("USER_WAVE1_ID"), col("Wave_1_count").cast(IntegerType))
    val expectedFirstWaveCount = Seq(
      Row(1, 3),
      Row(3, 3),
      Row(2, 2)
    )
    val expectedSchema = StructType(Array(
      StructField("USER_WAVE1_ID", IntegerType),
      StructField("Wave_1_count", IntegerType)
    ))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedFirstWaveCount), StructType(expectedSchema))
    assertSmallDatasetEquality(result, df, ignoreNullable = true)
  }
}