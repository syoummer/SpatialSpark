pomExtra := {
  <url>https://github.com/syoummer/SpatialSpark</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <url>github.com/syoummer/SpatialSpark.git</url>
      <connection>scm:git:github.com/syoummer/SpatialSpark.git</connection>
      <developerConnection>scm:git:git@github.com:syoummer/SpatialSpark.git</developerConnection>
    </scm>
    <developers>
      <developer>
        <id>syoummer</id>
        <name>Simin You</name>
        <url>http://simin.me</url>
      </developer>
      <developer>
        <id>kgs</id>
        <name>Kamil Gorlo</name>
        <url>http://kamilgorlo.com</url>
      </developer>
    </developers>
}

credentials ++= (for {
  username <- Option(System.getenv().get("SONATYPE_USERNAME"))
  password <- Option(System.getenv().get("SONATYPE_PASSWORD"))
} yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq