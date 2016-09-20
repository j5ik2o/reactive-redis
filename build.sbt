import com.typesafe.sbt.SbtScalariform.ScalariformKeys

import scalariform.formatter.preferences._

val akkaVersion = "2.4.10"

val formatPreferences = FormattingPreferences()
  .setPreference(RewriteArrowSymbols, false)
  .setPreference(AlignParameters, true)
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(SpacesAroundMultiImports, true)
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(AlignArguments, true)

lazy val commonSettings = Seq(
  organization := "com.github.j5ik2o"
  , version := "1.0.0"
  , scalaVersion := "2.11.8"
  , scalacOptions ++= Seq(
    "-feature"
    , "-deprecation"
    , "-unchecked"
    , "-encoding", "UTF-8"
    , "-Xfatal-warnings"
    , "-language:existentials"
    , "-language:implicitConversions"
    , "-language:postfixOps"
    , "-language:higherKinds"
  )
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
      , "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
    ),
    SbtScalariform.scalariformSettings ++ Seq(
      ScalariformKeys.preferences in Compile := formatPreferences
      , ScalariformKeys.preferences in Test := formatPreferences)
  )