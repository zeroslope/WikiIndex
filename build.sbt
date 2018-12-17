name := "wiki"

version := "0.5"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.3" % "provided"
libraryDependencies += "com.databricks" % "spark-xml_2.11" % "0.3.5"
libraryDependencies += "org.jsoup" % "jsoup" % "1.11.3"
libraryDependencies += "info.bliki.wiki" % "bliki-core" % "3.1.0"
libraryDependencies += "org.scala-lang" % "scala-reflect" % s"${scalaVersion}"