package cse511

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv")
      .option("delimiter",";")
      .option("header","false")
      .load(pointPath)
    pickupInfo.createOrReplaceTempView("nyctaxitrips")

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => (
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    ))
    spark.udf.register("CalculateY", (pickupPoint: String) => (
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    ))
    spark.udf.register("CalculateZ", (pickupTime: String) => (
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
    ))

    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5), CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    
    // Define the min and max of x, y, z
    val minX = (-74.50 / HotcellUtils.coordinateStep).toInt
    val maxX = (-73.70 / HotcellUtils.coordinateStep).toInt
    val minY = (40.50 / HotcellUtils.coordinateStep).toInt
    val maxY = (40.90 / HotcellUtils.coordinateStep).toInt
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)
    
    val cellCounts: DataFrame = HotcellUtils.createCellCountsView(spark, pickupInfo)
    // Step 2: Calculate the sum of neighboring cell pickup counts and number of neighboring cells
    val neighbors = cellCounts.as("c1").join(cellCounts.as("c2"))
      .filter(
        abs(col("c1.x") - col("c2.x")) <= 1 &&
        abs(col("c1.y") - col("c2.y")) <= 1 &&
        abs(col("c1.z") - col("c2.z")) <= 1
      )

    // Aggregate the results to match the SQL output
    val neighborsDF = neighbors.groupBy("c1.x", "c1.y", "c1.z")
      .agg(
        sum("c2.pickupCount").as("cellsInNeighbor"),
        count("*").as("numNeighborCells")
      )
    neighborsDF.createOrReplaceTempView("neighbors")

    // Step 3: Calculate the mean and standard deviation using HotcellUtils
    val stats = HotcellUtils.calculateStatistics(cellCounts, numCells)

    // Step 4: Compute the G-Score for each cell using HotcellUtils
    val gScoreDF = HotcellUtils.calculateGScore(spark, stats.mean, stats.stddev, numCells, neighborsDF)

    // Step 5: Return top 50 hottest cells sorted by G-score without showing G-score
    return gScoreDF.select("x", "y", "z").limit(50)
  }
}