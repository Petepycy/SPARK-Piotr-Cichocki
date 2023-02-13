package com.finalproject.spark

import com.SparkSessionWrapper
import org.apache.spark.sql.types.{FloatType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{avg, max, min, round}


object TotalAmount extends SparkSessionWrapper{

  import spark.implicits._
  case class Warehouse(positionId: Long, warehouse: String, product: String, eventTimeWarehouse: Long)
  case class Amount(positionId: Long, amount: Float, eventTimeAmount: Long)
  case class Output(positionId: Long, warehouse: String, product: String, currentAmount: Float)

  def createWarehouseDS(path: String): Dataset[Warehouse] = {
    val warehouseSchema = new StructType()
      .add("positionId", LongType, nullable = true)
      .add("warehouse", StringType, nullable = true)
      .add("product", StringType, nullable = true)
      .add("eventTimeWarehouse", LongType, nullable = true)

    val warehouses = spark.read
      .option("sep", ",")
      .schema(warehouseSchema)
      .csv(path)
      .as[Warehouse]
    warehouses
  }

  def createAmountDS(path: String): Dataset[Amount] = {
    val amountSchema = new StructType()
      .add("positionId", LongType, nullable = true)
      .add("amount", FloatType, nullable = true)
      .add("eventTimeAmount", LongType, nullable = false)

    val amounts = spark.read
      .option("sep", ",")
      .schema(amountSchema)
      .csv(path)
      .as[Amount]
    amounts
  }

  def findMaxMinAvg(allJoin: DataFrame): Dataset[Row] = allJoin
    .groupBy("warehouse", "product")
    .agg(max("amount").as("MAX"), min("amount").as("MIN"), round(avg("amount"), 3).as("AVERAGE"))
    .sort("product")

  def timestampMaxForAmount(amounts: Dataset[Amount]): Dataset[Row] = amounts
    .groupBy($"positionId")
    .max("eventTimeAmount")
    .as("lastUpdated")

  def currentAmount(warehouses: Dataset[Warehouse], afterFilter: DataFrame): Dataset[Output] = warehouses
    .join(afterFilter, usingColumn = "positionId")
    .select($"positionId", $"warehouse", $"product", $"amount".as("currentAmount"))
    .as[Output]

  def main(args: Array[String]): Unit = {

    println("\nLoading data...")

    val warehouses = createWarehouseDS("src/main/resources/warehouses.csv")
    val amounts = createAmountDS("src/main/resources/amount.csv")

    val all = warehouses.join(amounts, usingColumn = "positionId")

    val timestampMax = timestampMaxForAmount(amounts)

    val afterFilter = timestampMax
      .join(amounts, usingColumn = "positionId")
      .filter($"lastUpdated.max(eventTimeAmount)" === $"eventTimeAmount")
      .select($"positionId", $"amount")

    println("# current amount for each position, warehouse, product")
    val currentAmountForEach = currentAmount(warehouses, afterFilter)
    currentAmountForEach.show()

    println("# max, min, avg amounts for each warehouse and product")
    val result = findMaxMinAvg(all)
    result.show()

  }
}
