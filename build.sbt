val Organization = "me.simin"
val Version = "1.1.1-beta-SNAPSHOT"

val jts = "com.vividsolutions" % "jts" % "1.13"
val spark = "org.apache.spark" %% "spark-core" % "2.0.2" % "provided"
val sparkSQL = "org.apache.spark" %% "spark-sql" % "2.0.2" % "provided"
val scalaTest = "org.scalatest" %% "scalatest" % "2.2.4" % "test"
val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.3" % "test"

val buildSettings = Defaults.coreDefaultSettings ++ Seq(
  organization := Organization,
  version := Version,
  scalaVersion := "2.11.8",
  scalacOptions ++= Seq("-encoding", "UTF-8", "-unchecked", "-deprecation", "-feature"),
  parallelExecution := false
)

lazy val spatialSpark: Project = Project(
  "spatial-spark",
  file("."),
  settings = buildSettings ++ Seq(
    libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-compiler" % _ % "provided"),
    libraryDependencies ++= Seq(jts, scalaTest, sparkTestingBase, spark, sparkSQL))
)
