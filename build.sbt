val scalaVersion211 = "2.11.12"
val scalaVersion212 = "2.12.8"

val compileScalaStyle = taskKey[Unit]("compileScalaStyle")

lazy val scalaStyleSettings = Seq(
  (scalastyleConfig in Compile) := file("scalastyle-config.xml"),
  compileScalaStyle := scalastyle.in(Compile).toTask("").value,
  (compile in Compile) := (compile in Compile).dependsOn(compileScalaStyle).value
)

val coreSettings = Seq(
  sonatypeProfileName := "com.github.j5ik2o",
  organization := "com.github.j5ik2o",
  scalaVersion := scalaVersion211,
  crossScalaVersions ++= Seq(scalaVersion211, scalaVersion212),
  scalacOptions ++= {
    Seq(
      "-feature",
      "-deprecation",
      "-unchecked",
      "-encoding",
      "UTF-8",
      "-language:_",
    "-Ypartial-unification",
    "-Ydelambdafy:method",
    "-target:jvm-1.8"
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
  resolvers += Resolver.bintrayRepo("danslapman", "maven"),
  resolvers += Resolver.sonatypeRepo("releases"),
  addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.10"),
  libraryDependencies ++= Seq(
    "io.monix"       %% "monix"          % "3.0.0-RC2",
    "org.typelevel"  %% "cats-core"      % "1.5.0",
    "org.typelevel"  %% "cats-free"      % "1.5.0",
    "com.beachape"   %% "enumeratum"     % "1.5.13",
    "org.slf4j"      % "slf4j-api"       % "1.7.26",
    "org.scalatest"  %% "scalatest"      % "3.0.7" % Test,
    "org.scalacheck" %% "scalacheck"     % "1.14.0" % Test,
    "ch.qos.logback" % "logback-classic" % "1.2.3" % Test
  ),
  //  Global / concurrentRestrictions += Tags.limit(Tags.Test, 1),
  parallelExecution in Test := false,
  wartremoverErrors ++= Warts.allBut(
    Wart.Any,
    Wart.Throw,
    Wart.Nothing,
    Wart.Product,
    Wart.NonUnitStatements,
    Wart.DefaultArguments,
    Wart.ImplicitParameter,
    Wart.StringPlusAny,
    Wart.Overloading,
    Wart.Serializable,
    Wart.ImplicitConversion,
    Wart.Equals,
    Wart.EitherProjectionPartial,
    Wart.AsInstanceOf
  ),
  wartremoverExcluded += baseDirectory.value / "src" / "test" / "scala",
  scapegoatVersion in ThisBuild := "1.3.7"
) ++ scalaStyleSettings

val akkaVersion = "2.5.21"

lazy val test = (project in file("test"))
  .settings(
    coreSettings ++ Seq(
      name := "reactive-redis-test",
      libraryDependencies ++= Seq(
        "com.google.guava" % "guava"      % "25.1-jre",
        "commons-io"       % "commons-io" % "2.6",
        "org.scalatest"    %% "scalatest" % "3.0.7" % Provided
      )
    )
  )

lazy val core = (project in file("core")).settings(
  coreSettings ++ Seq(
    name := "reactive-redis-core",
    libraryDependencies ++= Seq(
      "com.typesafe.akka"  %% "akka-actor"                          % akkaVersion,
      "com.typesafe.akka"  %% "akka-testkit"                        % akkaVersion % Test,
      "com.typesafe.akka"  %% "akka-stream"                         % akkaVersion,
      "com.typesafe.akka"  %% "akka-slf4j"                          % akkaVersion,
      "com.lihaoyi"        %% "fastparse"                           % "1.0.0",
      "com.lihaoyi"        %% "fastparse-byte"                      % "1.0.0",
      "redis.clients"      % "jedis"                                % "2.9.0",
      "org.apache.commons" % "commons-lang3"                        % "3.8.1",
      "com.github.j5ik2o"  %% "akka-backoff-supervisor-enhancement" % "1.0.2"
    )
  )
) dependsOn (test % "test")

lazy val `pool-commons` = (project in file("pool-commons"))
  .settings(
    coreSettings ++ Seq(
      name := "reactive-redis-pool-commons",
      libraryDependencies ++= Seq(
        "org.apache.commons" % "commons-pool2" % "2.6.0"
      )
    )
  )
  .dependsOn(core % "compile;test->test")

lazy val `pool-fop` = (project in file("pool-fop"))
  .settings(
    coreSettings ++ Seq(
      name := "reactive-redis-pool-fop",
      libraryDependencies ++= Seq(
        "cn.danielw" % "fast-object-pool" % "2.1.0"
      )
    )
  )
  .dependsOn(core % "compile;test->test")

lazy val `pool-stormpot` = (project in file("pool-stormpot"))
  .settings(
    coreSettings ++ Seq(
      name := "reactive-redis-pool-stormpot",
      libraryDependencies ++= Seq(
        "com.github.chrisvest" % "stormpot" % "2.4.1"
      )
    )
  )
  .dependsOn(core % "compile;test->test")

lazy val `pool-scala` = (project in file("pool-scala"))
  .settings(
    coreSettings ++ Seq(
      name := "reactive-redis-pool-scala",
      libraryDependencies ++= Seq(
        "io.github.andrebeat" %% "scala-pool" % "0.4.1"
      )
    )
  )
  .dependsOn(core % "compile;test->test")

lazy val benchmark = (project in file("benchmark"))
  .settings(
    coreSettings ++ Seq(
      name := "reactive-redis-benchmark",
      libraryDependencies ++= Seq("com.github.etaty" %% "rediscala"   % "1.8.0",
                                  "redis.clients"    % "jedis"        % "2.9.0",
                                  "net.debasishg"    %% "redisclient" % "3.7")
    )
  )
  .enablePlugins(JmhPlugin)
  .dependsOn(core, test, `pool-commons`, `pool-fop`, `pool-scala`, `pool-stormpot`)

lazy val `root` = (project in file("."))
  .settings(coreSettings)
  .settings(
    name := "reactive-redis-project"
  )
  .aggregate(core, benchmark, `pool-commons`, `pool-fop`, `pool-scala`, `pool-stormpot`, test)
