package cse511

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART     
    // Parse the rectangle coordinates
      val rectangleCoords = queryRectangle.split(",").map(_.trim.toDouble)
      val (rxMin, ryMin, rxMax, ryMax) = (rectangleCoords(0), rectangleCoords(1), rectangleCoords(2), rectangleCoords(3))

      // Parse the point coordinates
      val pointCoords = pointString.split(",").map(_.trim.toDouble)
      val (px, py) = (pointCoords(0), pointCoords(1))

      // Check if the point is within the rectangle
      return (px >= rxMin && px <= rxMax && py >= ryMin && py <= ryMax)
  }
}
      