name := "spark-window-example"

version := "0.0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.0.0" % Test
)

initialCommands := "import sparkwindowexample._"

scalacOptions += "-target:jvm-1.8"

javaOptions ++= Seq(
  "-Xmx2048m",
  "-XX:+HeapDumpOnOutOfMemoryError"
)
