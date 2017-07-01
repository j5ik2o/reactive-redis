val akkaVersion = "2.4.10"

lazy val commonSettings = Seq(
  sonatypeProfileName := "com.github.j5ik2o",
  organization := "com.github.j5ik2o",
  scalaVersion := "2.11.8",
  crossScalaVersions := Seq("2.11.8", "2.12.1"),
  scalacOptions ++= Seq(
    "-feature",
    "-deprecation",
    "-unchecked",
    "-encoding",
    "UTF-8",
    "-Xfatal-warnings",
    "-language:existentials",
    "-language:implicitConversions",
    "-language:postfixOps",
    "-language:higherKinds"
  ),
  resolvers ++= Seq(
    "Sonatype OSS Release Repository" at "https://oss.sonatype.org/content/repositories/releases/"
  ),
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  ),
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ =>
    false
  },
  pomExtra := {
    <url>https://github.com/j5ik2o/reactive-redis</url>
      <licenses>
        <license>
          <name>The MIT License</name>
          <url>http://opensource.org/licenses/MIT</url>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:j5ik2o/reactive-redis.git</url>
        <connection>scm:git:github.com/j5ik2o/reactive-redis</connection>
        <developerConnection>scm:git:git@github.com:j5ik2o/reactive-redis.git</developerConnection>
      </scm>
      <developers>
        <developer>
          <id>j5ik2o</id>
          <name>Junichi Kato</name>
        </developer>
      </developers>
  },
  updateOptions := updateOptions.value.withCachedResolution(true),
  credentials := Def.task {
    val ivyCredentials = (baseDirectory in LocalRootProject).value / ".credentials"
    val result         = Credentials(ivyCredentials) :: Nil
    result
  }.value
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "reactive-redis-project"
  )
  .aggregate(core)

lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    name := "reactive-redis-core",
    parallelExecution in Test := false,
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor"          % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j"          % akkaVersion,
      "com.typesafe.akka" %% "akka-stream"         % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit"        % akkaVersion % "test",
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test",
      "org.slf4j"         % "slf4j-api"            % "1.7.21",
      "ch.qos.logback"    % "logback-classic"      % "1.1.7" % "provided",
      //"org.scalatest"          %% "scalatest"                % "3.0.1" % "test",
      "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
    )
  )
