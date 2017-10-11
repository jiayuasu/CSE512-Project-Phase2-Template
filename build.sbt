import sbt.Keys.{libraryDependencies, version}



lazy val root = (project in file(".")).
  settings(
    name := "CSE512-Project-Phase2-Template",

    version := "0.1.0",

    scalaVersion := "2.11.11",

    organization  := "org.datasyslab",

    publishMavenStyle := true
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided"
)