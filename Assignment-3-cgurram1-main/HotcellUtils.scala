package cse511

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int = {
    var result = 0
    coordinateOffset match {
      case 0 => result = Math.floor(inputString.split(",")(0).replace("(","").toDouble / coordinateStep).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble / coordinateStep).toInt
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Using day of the month for Z coordinate
      }
    }
    return result
  }

  def timestampParser(timestampString: String): java.sql.Timestamp = {
    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    new java.sql.Timestamp(parsedDate.getTime)
  }

  def dayOfMonth(timestamp: java.sql.Timestamp): Int = {
    val calendar = java.util.Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    calendar.get(java.util.Calendar.DAY_OF_MONTH)
  }

  case class Stats(mean: Double, stddev: Double)

  def calculateStatistics(cellCounts: DataFrame, numCells: Long): Stats = {
    val sumPoints = cellCounts.agg(sum("pickupCount")).first().getLong(0).toDouble
    val mean = sumPoints / numCells
    val squaredSum = cellCounts.agg(sum(pow("pickupCount", 2))).first().getDouble(0)
    val stddev = math.sqrt((squaredSum / numCells) - (mean * mean))
    
    Stats(mean, stddev)
  }
  def createCellCountsView(spark: SparkSession, pickupInfo: DataFrame): DataFrame = {
        pickupInfo.createOrReplaceTempView("cells")
        val cellCounts = spark.sql("""
          SELECT x, y, z, COUNT(*) AS pickupCount
          FROM cells
          GROUP BY x, y, z
        """)
        cellCounts.createOrReplaceTempView("cellCounts")
        cellCounts // Return the DataFrame
      }
  def calculateGScore(spark: SparkSession, mean: Double, stddev: Double, numCells: Long, neighborsDF: DataFrame): DataFrame = {
  // Calculate gScore using DataFrame API
  val gScoreDF = neighborsDF.select(
    col("x"),
    col("y"),
    col("z"),
    (
      (col("cellsInNeighbor") - (lit(mean) * col("numNeighborCells"))) /
      (lit(stddev) * sqrt((col("numNeighborCells") * lit(numCells) - col("numNeighborCells") * col("numNeighborCells")) / (lit(numCells) - 1)))
    ).alias("gscore")
  )
  .orderBy(desc("gscore"))

  gScoreDF
}
}