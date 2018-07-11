val coreSettings = Seq(
  sonatypeProfileName := "com.github.j5ik2o",
  organization := "com.github.j5ik2o",
  scalaVersion := "2.12.6",
  scalacOptions ++= {
    Seq(
      "-feature",
      "-deprecation",
      "-unchecked",
      "-encoding",
      "UTF-8",
      "-language:existentials",
      "-language:implicitConversions",
      "-language:postfixOps",
      "-language:higherKinds"
    ) ++ {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2L, scalaMajor)) if scalaMajor == 12 =>
          Seq.empty
        case Some((2L, scalaMajor)) if scalaMajor <= 11 =>
          Seq(
            "-Yinline-warnings"
          )
      }
    }
  },
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
  publishTo in ThisBuild := sonatypePublishTo.value,
  credentials := {
    val ivyCredentials = (baseDirectory in LocalRootProject).value / ".credentials"
    Credentials(ivyCredentials) :: Nil
  },
  scalafmtOnCompile in ThisBuild := true,
  scalafmtTestOnCompile in ThisBuild := true,
  libraryDependencies ++= Seq(
    "io.monix"       %% "monix"          % "3.0.0-RC1",
    "org.typelevel"  %% "cats-core"      % "1.1.0",
    "org.typelevel"  %% "cats-free"      % "1.1.0",
    "com.beachape"   %% "enumeratum"     % "1.5.13",
    "org.slf4j"      % "slf4j-api"       % "1.7.25",
    "org.scalatest"  %% "scalatest"      % "3.0.5" % Test,
    "org.scalacheck" %% "scalacheck"     % "1.14.0" % Test,
    "ch.qos.logback" % "logback-classic" % "1.2.3" % Test
  )
)

val akkaVersion = "2.5.11"

lazy val core = (project in file("core")).settings(
  coreSettings ++ Seq(
    name := "reactive-redis-core",
    libraryDependencies ++= Seq(
      "com.typesafe.akka"  %% "akka-actor"     % akkaVersion,
      "com.typesafe.akka"  %% "akka-testkit"   % akkaVersion % Test,
      "com.typesafe.akka"  %% "akka-stream"    % akkaVersion,
      "com.typesafe.akka"  %% "akka-slf4j"     % akkaVersion,
      "com.lihaoyi"        %% "fastparse"      % "1.0.0",
      "com.lihaoyi"        %% "fastparse-byte" % "1.0.0",
      "org.apache.commons" % "commons-pool2"   % "2.6.0"
    )
  )
)

lazy val `root` = (project in file("."))
  .settings(coreSettings)
  .settings(
    name := "reactive-redis-project"
  )
  .aggregate(core)
