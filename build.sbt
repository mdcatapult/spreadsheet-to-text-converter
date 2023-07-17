import sbtrelease.ReleaseStateTransformations._
import Release._

val meta = """META.INF(.)*""".r

lazy val IntegrationTest = config("it") extend Test

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    name              := "consumer-spreadsheetconverter",
    scalaVersion      := "2.13.3",
    scalacOptions ++= Seq(
      "-encoding", "utf-8",
      "-unchecked",
      "-deprecation",
      "-explaintypes",
      "-feature",
      "-Xlint"
    ),
    useCoursier      := false,
    resolvers         ++= Seq(
      "MDC Nexus Public" at "https://nexus.wopr.inf.mdc/repository/maven-public/",
      "MDC Nexus Snapshots" at "https://nexus.wopr.inf.mdc/repository/maven-snapshots/",
      "Maven Public" at "https://repo1.maven.org/maven2"),
    updateOptions     := updateOptions.value.withLatestSnapshots(latestSnapshots = false),
    credentials       += {
      sys.env.get("NEXUS_PASSWORD") match {
        case Some(p) =>
          Credentials("Sonatype Nexus Repository Manager", "nexus.wopr.inf.mdc", "gitlab", p)
        case None =>
          Credentials(Path.userHome / ".sbt" / ".credentials")
      }
    },
    libraryDependencies ++= {
      val doclibCommonVersion = "4.0.0"

      val configVersion = "1.4.2"
      val akkaVersion = "2.8.1"
      val catsVersion = "2.9.0"
      val apachePoiVersion = "4.1.2"
      val apachePoiXMLVersion = "4.1.2"
      val scalacticVersion = "3.2.15"
      val scalaTestVersion = "3.2.15"
      val scalaMockVersion = "5.2.0"
      val scalaLoggingVersion = "3.9.5"
      val logbackClassicVersion = "1.4.7"
      val sodsVersion = "1.5.2"
      val log4jVersion = "2.20.0"

      Seq(
        "org.scalactic" %% "scalactic"                  % scalacticVersion,
        "org.scalatest" %% "scalatest"                  % scalaTestVersion % "it,test",
        "org.scalamock" %% "scalamock"                  % scalaMockVersion % "it,test",
        "com.typesafe.akka" %% "akka-testkit"           % akkaVersion % "it,test",
        "com.typesafe.akka" %% "akka-slf4j"             % akkaVersion,
        "ch.qos.logback" % "logback-classic"            % logbackClassicVersion,
        "org.apache.logging.log4j" % "log4j-core"         % log4jVersion,
        "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
        "com.typesafe" % "config"                       % configVersion,
        "org.typelevel" %% "cats-kernel"                % catsVersion,
        "org.typelevel" %% "cats-core"                  % catsVersion,
        "io.mdcatapult.doclib" %% "common"              % doclibCommonVersion,
        "org.apache.poi" % "poi"                        % apachePoiVersion,
        "org.apache.poi" % "poi-ooxml"                  % apachePoiVersion,
        "org.apache.poi" % "poi-ooxml-schemas"          % apachePoiXMLVersion,
        "com.github.miachm.sods" % "SODS"               % sodsVersion
      )
    }.map(
      _.exclude(org = "javax.ws.rs", name = "javax.ws.rs-api")
        .exclude(org = "com.google.protobuf", name = "protobuf-java")
        .exclude(org = "com.typesafe.play", name = "shaded-asynchttpclient")
    ),
  )
  .settings(
    assemblyJarName := "consumer.jar",
    assembly / test:= {},
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", "INDEX.LIST") => MergeStrategy.discard
      case PathList("com", "sun", _*) => MergeStrategy.first
      case PathList("javax", "servlet", _*) => MergeStrategy.first
      case PathList("javax", "activation", _*) => MergeStrategy.first
      case PathList("org", "apache", "commons", _*) => MergeStrategy.first
      case PathList("com", "ctc", "wstx", _*) => MergeStrategy.first
      case PathList("scala", "collection", "compat", _*) => MergeStrategy.first
      case PathList("scala", "util", "control", "compat", _*) => MergeStrategy.first
      case PathList(xs @ _*) if xs.last endsWith ".DSA" => MergeStrategy.discard
      case PathList(xs @ _*) if xs.last endsWith ".SF" => MergeStrategy.discard
      case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
      case PathList(xs @ _*) if xs.last == "module-info.class" => MergeStrategy.first
      case PathList(xs @ _*) if xs.last == "public-suffix-list.txt" => MergeStrategy.first
      case PathList(xs @ _*) if xs.last == ".gitkeep" => MergeStrategy.discard
      case PathList("org", "w3c", "dom", "UserDataHandler.class") => MergeStrategy.first
      case n if n.startsWith("application.conf") => MergeStrategy.first
      case n if n.endsWith(".conf") => MergeStrategy.concat
      case n if n.startsWith("logback.xml") => MergeStrategy.first
      case n if n.startsWith("scala-collection-compat.properties") => MergeStrategy.first
      case meta(_) => MergeStrategy.first
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )
  .settings(
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      runTest,
      setReleaseVersion,
      getShortSha,
      writeReleaseVersionFile,
      commitAllRelease,
      tagRelease,
      runAssembly,
      setNextVersion,
      writeNextVersionFile,
      commitAllNext,
      pushChanges
    )
  )



