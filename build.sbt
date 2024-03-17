ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "SpaBiMeta0.1.2"
  )
val sparkVersion = "3.2.1"
resolvers += "Spark Packages Repo" at "https://repos.spark-packages.org/"
libraryDependencies ++= Seq(
  "graphframes" % "graphframes" % "0.8.2-spark3.2-s_2.12",
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion
)