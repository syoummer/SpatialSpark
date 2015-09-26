name := "spatial-spark"

version := "1.1-SNAPSHOT"

resolvers += "Akka Repository" at "http://repo.akka.io/releases"

libraryDependencies ++= Seq(
  "com.vividsolutions" % "jts" % "1.13",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "1.3.0_0.0.5" % "test",
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"
)
