name := "test"


version := "1.0"

//scalaVersion := "2.11.8"
scalaVersion := "2.10.4"


libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"

libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.6.1" % "provided"


//libraryDependencies += "com.snowplowanalytics" %% "scala-maxmind-iplookups"  % "1.2.0"

//libraryDependencies += "com.sanoma.cda" %% "maxmind-geoip2-scala" % "1.5.1"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.1" % "provided"

//libraryDependencies += "org.json" % "json" % "20160212"



resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

//resolvers += "SnowPlow Repo" at "http://maven.snplow.com/releases/"

//resolvers += "Twitter Maven Repo" at "http://maven.twttr.com/"
