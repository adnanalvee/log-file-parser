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
)

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "MrPowers" % "spark-fast-tests" % "2.2.0_0.5.0" % "test",
    "com.typesafe" % "config" % "1.3.1",
    "com.github.scopt" %% "scopt" % "3.6.0"
  )

  // META-INF discarding
  mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
     {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
     }
  }
