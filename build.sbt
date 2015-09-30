val Organization = "me.simin"
val Version = "1.1-SNAPSHOT"

val jts = "com.vividsolutions" % "jts" % "1.13"
val scalaTest = "org.scalatest" %% "scalatest" % "2.2.4" % "test"
val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % "1.3.0_0.0.5" % "test"
val spark = "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"

val buildSettings = Defaults.coreDefaultSettings ++ Seq(
  organization := Organization,
  version := Version,
  scalaVersion := "2.10.5",
  scalacOptions ++= Seq("-encoding", "UTF-8", "-unchecked", "-deprecation", "-feature"),
  parallelExecution := false
)

lazy val spatialSpark: Project = Project(
  "spatial-spark",
  file("."),
  settings = buildSettings ++ Seq(
    libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-compiler" % _ % "provided"),
    libraryDependencies ++= Seq(jts, scalaTest, sparkTestingBase, spark))
)