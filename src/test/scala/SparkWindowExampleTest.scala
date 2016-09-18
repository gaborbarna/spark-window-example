package sparkwindowexample

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.apache.spark.sql.SparkSession

class SparkWindowExampleTest extends FunSuite with BeforeAndAfterAll {
  @transient lazy val spark: SparkSession = SparkSessionProvider._sparkSession

  override def beforeAll() {
    super.beforeAll()
    val builder = SparkSession.builder().master("local")
    SparkSessionProvider._sparkSession = builder.getOrCreate()
  }

  override def afterAll() {
    spark.stop
    SparkSessionProvider._sparkSession = null
    super.afterAll()
  }

  test("test calculate") {
    import spark.implicits._

    val rw = RollingWindow(spark)
    val testInputDF = {
      val lines = getResource("/test_input.txt")
      val df = spark.sparkContext.parallelize(lines).toDF("value")
      rw.parseInput(df)
    }
    val testOutputDF = {
      val lines = getResource("/test_output.txt")
      spark.sparkContext.parallelize(lines).map(_.split(" ") match {
        case Array(t, v, n, rs, minV, maxV) =>
          (t.toLong, v.toDouble, n.toLong, rs.toDouble, minV.toDouble, maxV.toDouble)
      }).toDF("T", "V", "N", "RS", "minV", "maxV")
    }
    assert(rw.calculate(testInputDF).except(testOutputDF).count == 0)
  }

  private def getResource(path: String) = {
    val stream = this.getClass.getResourceAsStream(path)
    scala.io.Source.fromInputStream(stream).getLines.toSeq
  }
}

object SparkSessionProvider {
  @transient var _sparkSession: SparkSession = _
  def sparkSession = _sparkSession
}
