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
    resultDf.write.csv(args(2)+"/output.csv")
    spark.stop()
  }

  private def runRangeQuery(spark: SparkSession,args: Array[String]): Long = {

    val arealmDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(args(0));
    arealmDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((true)))

    val resultDf = spark.sql("select * from point where ST_Contains(point._c0,'-155.940114,19.081331,-155.618917,19.5307')")
    resultDf.show()

    return resultDf.count()
  }

  private def runRangeJoinQuery(spark: SparkSession,args: Array[String]): Long = {

    val arealmDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(args(0));
    arealmDf.createOrReplaceTempView("point")

    val zcta510Df = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(args(1));
    zcta510Df.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(rectanlgeString:String, pointString:String)=>((true)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  private def runDistanceQuery(spark: SparkSession,args: Array[String]): Long = {

    val arealmDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(args(0));
    arealmDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((true)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'-88.331492,32.324142',10)")
    resultDf.show()

    return resultDf.count()
  }



  private def runDistanceJoinQuery(spark: SparkSession,args: Array[String]): Long = {

    val arealmDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(args(0));
    arealmDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((true)))
    val resultDf = spark.sql("select * from point p1, point p2 where ST_Within(p1._c0, p2._c0, 10)")
    resultDf.show()

    return resultDf.count()
  }
}
