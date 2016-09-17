val akkaVersion = "2.4.10"

lazy val commonSettings = Seq(
  organization := "com.github.j5ik2o",
  version := "1.0.0",
  scalaVersion := "2.11.8"
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "reactive-redis"
  ).aggregate(core)

lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    name := "reactive-redis-core",
    parallelExecution in Test := false,
    libraryDependencies := Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion
      , "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
      , "com.typesafe.akka" %% "akka-stream" % akkaVersion
      , "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
      , "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test"
      , "org.slf4j" % "slf4j-api" % "1.7.21"
      , "ch.qos.logback" % "logback-classic" % "1.1.7"
      , "org.scalatest" %% "scalatest" % "3.0.0" % "test"
    )
  )