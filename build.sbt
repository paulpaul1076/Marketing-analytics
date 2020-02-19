name := "Marketing analytics"

version := "0.1"

scalaVersion := "2.11.1"

mainClass in Compile := Some("com.company.MainNonSQL")
assemblyJarName in assembly := "MarketingAnalytics.jar"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided",
  "junit" % "junit" % "4.12" % Test,
  "commons-cli" % "commons-cli" % "1.4"
)

dependencyOverrides += "commons-cli" % "commons-cli" % "1.4"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}