name := "btree"

version := "0.1-SNAPSHOT"

organization := "com.zilverline"

description := "Memory efficient In-Memory B-Trees."

scalaVersion := "2.10.2"

scalacOptions := Seq("-deprecation", "-feature", "-unchecked", "-Xlint")

testFrameworks in Test := Seq(TestFrameworks.Specs2)

libraryDependencies := Seq(
  "org.scala-lang" % "scala-library" % "2.10.2",
  "org.specs2" %% "specs2" % "2.1" % "test",
  "org.scalacheck" %% "scalacheck" % "1.10.1" % "test")
