package cse511

import org.apache.spark.sql.{SaveMode, SparkSession}

object SpatialQuery extends App {

 
  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    val Array(x, y) = pointString.split(",").map(_.toDouble)
    val Array(xMin, yMin, xMax, yMax) = queryRectangle.split(",").map(_.toDouble)
    x >= xMin && x <= xMax && y >= yMin && y <= yMax
  }

  
  def ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean = {
    val Array(x1, y1) = pointString1.split(",").map(_.toDouble)
    val Array(x2, y2) = pointString2.split(",").map(_.toDouble)
    math.sqrt(math.pow(x2 - x1, 2) + math.pow(y2 - y1, 2)) <= distance
  }

  def runRangeQuery(spark: SparkSession, pointFilePath: String, queryRectangle: String): Long = {
    val points = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load(pointFilePath)
    points.createOrReplaceTempView("point")

    spark.udf.register("ST_Contains", (rect: String, point: String) => ST_Contains(rect, point))

    val result = spark.sql(s"SELECT * FROM point WHERE ST_Contains('$queryRectangle', point._c0)")
    
    val count = result.count()
    println(count)
    
    result.show()
    
    count
  }

  def runRangeJoinQuery(spark: SparkSession, pointFilePath: String, rectangleFilePath: String): Long = {
    val points = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load(pointFilePath)
    val rectangles = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load(rectangleFilePath)

    points.withColumnRenamed("_c0", "point").createOrReplaceTempView("point")
    rectangles.withColumnRenamed("_c0", "rectangle").createOrReplaceTempView("rectangle")

    spark.udf.register("ST_Contains", (rect: String, point: String) => ST_Contains(rect, point))

    val result = spark.sql("SELECT * FROM rectangle, point WHERE ST_Contains(rectangle.rectangle, point.point)")

    val count = result.count()
    println(count)

    result.show()

    count
  }

  // Function to execute a distance-based query
  def runDistanceQuery(spark: SparkSession, pointFilePath: String, referencePoint: String, maxDistance: String): Long = {
    val points = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load(pointFilePath)
    points.createOrReplaceTempView("point")

    spark.udf.register("ST_Within", (point1: String, point2: String, dist: Double) => ST_Within(point1, point2, dist))

    val result = spark.sql(s"SELECT * FROM point WHERE ST_Within(point._c0, '$referencePoint', $maxDistance)")

    val count = result.count()
    println(count)

    result.show()
    
    count
  }

  // Function to execute a distance join query between two point datasets
  def runDistanceJoinQuery(spark: SparkSession, pointFilePath1: String, pointFilePath2: String, maxDistance: String): Long = {
    val points1 = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load(pointFilePath1)
    val points2 = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load(pointFilePath2)

    points1.withColumnRenamed("_c0", "point1").createOrReplaceTempView("point1")
    points2.withColumnRenamed("_c0", "point2").createOrReplaceTempView("point2")

    spark.udf.register("ST_Within", (point1: String, point2: String, dist: Double) => ST_Within(point1, point2, dist))

    val result = spark.sql(s"SELECT * FROM point1 p1, point2 p2 WHERE ST_Within(p1.point1, p2.point2, $maxDistance)")

    val count = result.count()
    println(count)

    result.show()

    count
  }
}