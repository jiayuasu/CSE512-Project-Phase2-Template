# CSE512-Project-Phase2-Template

## Requirement

In Project Phase 2, you need to write two User Defined Functions (ST\_Contains, ST\_Within) in SparkSQL and use them to do four spatial queries:

* Range query: Use ST_Contains
* Range join query: Use ST_Contains
* Distance query: Use ST_Within
* Distance join query: Use ST_Within

You **only have to replace the logic** (currently is "true") in all User Defined Function and then submit it using "spark-submit".

Their interfaces are as follows:

### 1. ST_Contains


Input: pointString:String, queryRectangle:String

Output: Boolean (true or false)

Definition: Check whether queryRectangle fully contains point. Consider on-boundary point.

### 2. ST_Within

Input: pointString1:String, pointString2:String, distance:Double

Output: Boolean (true or false)

Definition: Check whether two points are within the given distance. Consider on-boundary point.


### 3. Use Your UDF in SparkSQL

Range query:
```
select * 
from point where ST_Contains(point._c0,'-155.940114,19.081331,-155.618917,19.5307')
```

Range join query:
```
select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)
```

Distance query:
```
select * from point where ST_Within(point._c0,'-88.331492,32.324142',10)
```

Distance join query:
```
select * from point p1, point p2 where ST_Within(p1._c0, p2._c0, 10)
```

### 4. Print the count of your result DataFrame
You must print the count of your resultDataFrame. Code is commentted out in the template.

```
print(resultDf.count())
```
### 5. Run your code on Apache Spark using "spark-submit"

## How to debug your code in IDE

If you are using this Scala template

1. Use IntelliJ Idea with Scala plug-in or any other Scala IDE.
2. Replace the logic of User Defined Functions (ST\_Contains, ST\_Within) in SparkSQLExample.scala.
3. In some cases, you may need to go to "build.sbt" file and change ```% "provided"``` to ```% "compile"``` in order to debug your code in IDE
4. Run your code in IDE

You can also use Java. But you must complete all the required tasks.

## How to submit your code to Spark
If you are using this Scala template

1. Go to project root folder
2. Run ```sbt assembly```
3. Find the packaged jar in "./target/scala-2.11/CSE512-Project-Phase2-Template-assembly-0.1.0.jar"
4. Submit the jar to Spark using Spark command "./bin/spark-submit"
