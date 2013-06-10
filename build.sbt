organization := "com.intel"

name := "kafka-yarn"

version := "0.0.1-SNAPSHOT"

description := "Kafka On Yarn"

homepage := Some(url("http://kkasravi.github.com/kafka-yarn"))

startYear := Some(2013)

licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

organizationName := "Intel"

organizationHomepage := Some(url("http://intel.com"))

resolvers += "Hadoop" at "http://repo1.maven.org/maven2/"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.0.4-alpha",
  "org.apache.hadoop" % "hadoop-yarn-common" % "2.0.4-alpha",
  "org.apache.hadoop" % "hadoop-yarn-client" % "2.0.4-alpha"
)

// for tests
libraryDependencies ++= Seq(
  "org.specs2" %% "specs2" % "1.9" % "test",
  "junit" % "junit" % "4.8.2" % "test")

testOptions in Test += Tests.Argument("console", "junitxml")

EclipseKeys.withSource := true
