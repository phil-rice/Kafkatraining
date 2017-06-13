name := "kafka"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.10.2.1"

libraryDependencies += "org.scala-lang.modules" %% "scala-java8-compat" % "0.8.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
