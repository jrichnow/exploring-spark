name := "exploring-spark"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.1.0" withSources,
                            "net.liftweb" %% "lift-json" % "2.5.1",
                            "org.scalatest" % "scalatest_2.10" % "2.2.0" % "test",
                            "joda-time" % "joda-time" % "2.5")
