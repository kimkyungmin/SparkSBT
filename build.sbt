name := "SparkSBT"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0"
  , "org.scala-lang" % "scala-compiler" % "2.11.8"
  , "org.apache.commons" % "commons-lang3" % "3.3.2"
  , "jline" % "jline" % "2.12.1" % "provided"
  , "org.slf4j" % "slf4j-api" % "1.7.10"
)

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.0"
  , "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.4.0"
)

libraryDependencies += "au.com.bytecode" % "opencsv" % "2.4"
