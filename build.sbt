name := "SpatialSpark"

version := "1.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases"

libraryDependencies += "com.vividsolutions" % "jts" % "1.13"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0" % "provided"

