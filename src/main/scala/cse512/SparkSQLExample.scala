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
      .config("spark.some.config.option", "some-value").master(args(0))
      .getOrCreate()


    runRangeQuery(spark,args)
    runRangeJoinQuery(spark,args)
    runDistanceQuery(spark,args)
    runDistanceJoinQuery(spark,args)

    spark.stop()
  }

  private def runRangeQuery(spark: SparkSession,args: Array[String]): Unit = {

    val arealmDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(args(1));
    arealmDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((true)))

    val resultDf = spark.sql("select * from point where ST_Contains(point._c0,'-155.940114,19.081331,-155.618917,19.5307')")
    resultDf.show()
    //print(resultDf.count())
  }

  private def runRangeJoinQuery(spark: SparkSession,args: Array[String]): Unit = {

    val arealmDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(args(1));
    arealmDf.createOrReplaceTempView("point")

    val zcta510Df = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(args(2));
    zcta510Df.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(rectanlgeString:String, pointString:String)=>((true)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()
    //print(resultDf.count())
  }

  private def runDistanceQuery(spark: SparkSession,args: Array[String]): Unit = {

    val arealmDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(args(1));
    arealmDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((true)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'-88.331492,32.324142',10)")
    resultDf.show()
    //print(resultDf.count())
  }



  private def runDistanceJoinQuery(spark: SparkSession,args: Array[String]): Unit = {

    val arealmDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(args(1));
    arealmDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((true)))
    val resultDf = spark.sql("select * from point p1, point p2 where ST_Within(p1._c0, p2._c0, 10)")

    resultDf.show()
    //print(resultDf.count())
  }
}
