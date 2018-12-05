lazy val root = (project in file(".")).
  settings(
  name := "silvertail-parser",
  version := "0.1.0",
  scalaVersion := "2.11.0",
  assemblyJarName in assembly := "Adnan.jar",
  mainClass in Compile := Some("com.att.cdo.security.Main")
  )

val sparkVersion = "2.1.1"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
  "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
)

updateOptions := updateOptions.value.withGigahorse(false)

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "MrPowers" % "spark-fast-tests" % "0.17.1-s_2.11",
    "com.typesafe" % "config" % "1.3.1",
    "com.github.scopt" %% "scopt" % "3.6.0"
  )


