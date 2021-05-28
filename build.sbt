name := "simba"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.3"
libraryDependencies += "org.apache.spark" % "spark-catalyst_2.10" % "1.6.3"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.3"

libraryDependencies += "com.vividsolutions" % "jts-core" % "1.14.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

mainClass in Compile := Some("edu.utah.cs.simba.examples.TestMain")

//assemblyMergeStrategy in assembly := {
//  case PathList("org", "apache", "spark", xs @ _*)         => MergeStrategy.last
//  case PathList("org", "apache", "commons", xs @ _*)         => MergeStrategy.first
//  case PathList("org", "apache", "hadoop", xs @ _*)         => MergeStrategy.first
//  case PathList("com", "google", xs @ _*)         => MergeStrategy.first
//  case PathList("com", "esotericsoftware", xs @ _*)         => MergeStrategy.first
//  case PathList(ps @ _*) if ps.last endsWith "axiom.xml" => MergeStrategy.filterDistinctLines
//  case PathList(ps @ _*) if ps.last endsWith "Log$Logger.class" => MergeStrategy.first
//  case PathList(ps @ _*) if ps.last endsWith "ILoggerFactory.class" => MergeStrategy.first
//  case x =>
//    val oldStrategy = (assemblyMergeStrategy in assembly).value
//    oldStrategy(x)
//}
