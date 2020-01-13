import sbtrelease.ReleaseStateTransformations._
import Release._

lazy val configVersion = "1.3.2"
lazy val akkaVersion = "2.5.25"
lazy val catsVersion = "2.0.0"
lazy val opRabbitVersion = "2.1.0"
lazy val mongoVersion = "2.7.0"
lazy val awsScalaVersion = "0.8.1"
lazy val tikaVersion = "1.21"
lazy val apachePoiVersion = "4.1.0"
lazy val playTestVersion = "4.0.0"
lazy val doclibCommonVersion = "0.0.26"

val meta = """META.INF(.)*""".r

lazy val IntegrationTest = config("it") extend Test

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    name              := "consumer-spreadsheetconverter",
    scalaVersion      := "2.12.8",
    scalacOptions     ++= Seq("-Ypartial-unification"),
    resolvers         ++= Seq(
      "MDC Nexus Public" at "https://nexus.mdcatapult.io/repository/maven-public/",
      "MDC Nexus Snapshots" at "https://nexus.mdcatapult.io/repository/maven-snapshots/",
      "Maven Public" at "https://repo1.maven.org/maven2"),
    updateOptions     := updateOptions.value.withLatestSnapshots(false),
    credentials       += {
      val nexusPassword = sys.env.get("NEXUS_PASSWORD")
      if ( nexusPassword.nonEmpty ) {
        Credentials("Sonatype Nexus Repository Manager", "nexus.mdcatapult.io", "gitlab", nexusPassword.get)
      } else {
        Credentials(Path.userHome / ".sbt" / ".credentials")
      }
    },
    libraryDependencies ++= Seq(
      "org.scalactic" %% "scalactic"                  % "3.0.5",
      "org.scalatest" %% "scalatest"                  % "3.0.5" % "it, test",
      "org.scalamock" %% "scalamock"                  % "4.3.0" % "it,test",
      "com.typesafe.akka" %% "akka-testkit"           % akkaVersion % "it,test",
      "com.typesafe.akka" %% "akka-slf4j"             % akkaVersion,
      "ch.qos.logback" % "logback-classic"            % "1.2.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "com.typesafe" % "config"                       % configVersion,
      "org.typelevel" %% "cats-macros"                % catsVersion,
      "org.typelevel" %% "cats-kernel"                % catsVersion,
      "org.typelevel" %% "cats-core"                  % catsVersion,
      "io.mdcatapult.doclib" %% "common"              % doclibCommonVersion,
      "org.apache.commons" % "commons-compress"       % "1.18",
      "org.apache.tika" % "tika-core"                 % tikaVersion,
      "org.apache.tika" % "tika-parsers"              % tikaVersion,
      "org.apache.tika" % "tika-langdetect"           % tikaVersion,
      "org.apache.poi" % "poi"                        % apachePoiVersion,
      "org.apache.poi" % "poi-ooxml"                  % apachePoiVersion,
      "org.apache.poi" % "poi-ooxml-schemas"          % apachePoiVersion,
      "org.apache.pdfbox" % "jbig2-imageio"           % "3.0.2",
      "com.github.jai-imageio" % "jai-imageio-jpeg2000" % "1.3.0",
      "org.xerial" % "sqlite-jdbc"                      % "3.25.2",
      "jakarta.ws.rs" % "jakarta.ws.rs-api" % "2.1.6",
      "com.github.miachm.sods" % "SODS" % "1.2.1",
    ).map(_ exclude("javax.ws.rs", "javax.ws.rs-api")),
  )
  .settings(
    assemblyJarName := "consumer.jar",
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", "INDEX.LIST") => MergeStrategy.discard
      case PathList("com", "sun", _*) => MergeStrategy.first
      case PathList("javax", "servlet", _*) => MergeStrategy.first
      case PathList("javax", "activation", _*) => MergeStrategy.first
      case PathList("org", "apache", "commons", _*) => MergeStrategy.first
      case PathList("com", "ctc", "wstx", _*) => MergeStrategy.first
      case PathList(xs @ _*) if xs.last endsWith ".DSA" => MergeStrategy.discard
      case PathList(xs @ _*) if xs.last endsWith ".SF" => MergeStrategy.discard
      case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
      case PathList(xs @ _*) if xs.last == "module-info.class" => MergeStrategy.first
      case PathList(xs @ _*) if xs.last == "public-suffix-list.txt" => MergeStrategy.first
      case PathList(xs @ _*) if xs.last == ".gitkeep" => MergeStrategy.discard
      case PathList("org", "w3c", "dom", "UserDataHandler.class") => MergeStrategy.first
      case n if n.startsWith("application.conf") => MergeStrategy.concat
      case n if n.endsWith(".conf") => MergeStrategy.concat
      case meta(_) => MergeStrategy.first
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
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



