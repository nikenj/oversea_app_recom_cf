name := "oversea-cf-item-similar"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1" % "provided",
  "org.apache.spark" % "spark-hive_2.10" % "1.6.1" % "provided",
  "com.typesafe" % "config" % "1.2.1",
  "joda-time" % "joda-time" % "2.9"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)