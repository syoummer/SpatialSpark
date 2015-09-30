logLevel := Level.Warn

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.11.2")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "0.5.1")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")