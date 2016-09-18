package sparkwindowexample

import org.apache.spark.sql.{SparkSession, Row}

object SparkWindowExample {
  def main(args: Array[String]): Unit = args match {
    case Array(inputPath, _*) => {
      val spark = getSparkSession
      val rw = RollingWindow(spark)
      val inputDF = spark.read.text(inputPath)
      val resultDF = (rw.parseInput _ andThen rw.calculate _ andThen rw.formatOutput _)(inputDF)
      println("T" + " " * 10 + "V       N RS      MinV    MaxV\n" + "-" * 44)
      resultDF.foreach((row: Row) => println(row.getAs[String]("value")))
      spark.stop
    }
    case _ => println("input path not specified")
  }

  private def getSparkSession = SparkSession.builder
    .master("local")
    .appName("spark-window-example")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
}
