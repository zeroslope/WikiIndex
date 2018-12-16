name := "wiki"

version := "0.2"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.3" % "provided"
libraryDependencies += "com.databricks" % "spark-xml_2.11" % "0.3.5"
