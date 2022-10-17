ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "Newday_Coding_Challenge",
    idePackagePrefix := Some("com.scala.task")
  )
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.2"