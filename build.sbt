name := "exploring-spark"

version := "1.0"

scalaVersion := "2.11.1"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.4.0" withSources,
                            "net.liftweb" %% "lift-json" % "2.6",
                            "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test",
                            "joda-time" % "joda-time" % "2.5",
                            "org.yaml" % "snakeyaml" % "1.10",
                            "com.github.seratch" %% "awscala" % "0.3.+" withSources,
                            "org.apache.devicemap" % "devicemap-client" % "1.1.0",
                            "org.apache.devicemap" % "devicemap-data" % "1.0.3")
