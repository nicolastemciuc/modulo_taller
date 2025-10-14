name := "Kmeans"

version := "1.0"

scalaVersion := "2.12.15"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.4.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.4.1"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", _*) => MergeStrategy.discard
 case _                        => MergeStrategy.first
}
