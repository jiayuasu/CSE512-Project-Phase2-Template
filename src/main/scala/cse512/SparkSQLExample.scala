package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object SparkSQLExample {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("CSE512-Phase2")
      .config("spark.some.config.option", "some-value")//.master(args(0))
      .getOrCreate()

    var result:Array[String] = new Array[String](4)

    result(0)=runRangeQuery(spark,args).toString
    result(1)=runRangeJoinQuery(spark,args).toString
    result(2)=runDistanceQuery(spark,args).toString
    result(3)=runDistanceJoinQuery(spark,args).toString
    import spark.implicits._
    val resultDf = Seq(result(0),result(1),result(2),result(3)).toDF()
    resultDf.write.csv(args(0))
    spark.stop()
  }

  private def runRangeQuery(spark: SparkSession,args: Array[String]): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(args(1));
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((true)))

    val resultDf = spark.sql("select * from point where ST_Contains(point._c0,'"+args(2)+"')")
    resultDf.show()

    return resultDf.count()
  }

  private def runRangeJoinQuery(spark: SparkSession,args: Array[String]): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(args(3));
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(args(4));
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(rectanlgeString:String, pointString:String)=>((true)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  private def runDistanceQuery(spark: SparkSession,args: Array[String]): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(args(5));
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((true)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+args(6)+"',"+args(7)+")")
    resultDf.show()

    return resultDf.count()
  }

  private def runDistanceJoinQuery(spark: SparkSession,args: Array[String]): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(args(8));
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(args(9));
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((true)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+args(10)+")")
    resultDf.show()

    return resultDf.count()
  }
}
