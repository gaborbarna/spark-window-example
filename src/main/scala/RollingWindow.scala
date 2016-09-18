package sparkwindowexample

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

case class RollingWindow(spark: SparkSession) {
  import spark.implicits._
  import RollingWindow._

  def calculate(df: DataFrame) = {
    val w1 = Window.partitionBy('T).orderBy('T).rowsBetween(Long.MinValue, 0)
    val w2 = Window.orderBy('T).rangeBetween(-60l, -1l)
    val result1 = selectOverWindow(df, w1)
    val result2 = selectOverWindow(df, w2)
    val joined = result1.join(result2, result1("RN") === result2("RN"))
    joined.map {
      case Row(t: Long, v: Double, n1: Long, rs1: Double, minV1: Double, maxV1: Double, _: Int,
        _: Long, _: Double, n2: Long, rs2: Double, minV2: Double, maxV2: Double, _: Int) =>
        ResultRow(t, v, n1 + n2, roundAt5(rs1 + rs2), Math.min(minV1, minV2), Math.max(maxV1, maxV2))
    }.toDF
  }

  def parseInput(df: DataFrame) =
    df.map(_.getAs[String]("value").split("\\s+") match {
      case Array(ts, price) => (ts.toLong, price.toDouble)
    }).toDF("T", "V")

  def formatOutput(df: DataFrame) =
    df.map { case Row(t: Long, v: Double, n: Long, rs: Double, minV: Double, maxV: Double) =>
      f"$t%10d $v%1.5f $n%1d $rs%1.5f $minV%1.5f $maxV%1.5f"
    }.toDF("value")

  private def selectOverWindow(df: DataFrame, w: WindowSpec) =
    df.select(
      '*,
      count('*).over(w) as 'N,
      sum('V).over(w) as 'RS,
      min('V).over(w) as 'minV,
      max('V).over(w) as 'maxV,
      row_number.over(Window.orderBy('T)) as 'RN)
      .withColumn("RS", when(isnull('RS), 0.0).otherwise('RS))
      .withColumn("minV", when(isnull('minV), Double.MaxValue).otherwise('minV))
      .withColumn("maxV", when(isnull('maxV), Double.MinValue).otherwise('maxV))

  private def roundAt(p: Int)(n: Double) = {
    val s = math pow (10, p)
    (math round n * s) / s
  }

  private def roundAt5 = roundAt(5) _
}

object RollingWindow {
  case class ResultRow(T: Long, V: Double, N: Long, RS: Double, minV: Double, maxV: Double)
}
