import cats.syntax.either._
import cats.syntax.functorFilter._
import sbt.{io => _, _}

object Dependencies {
  val daml_libraries_version = metabuild.BuildInfo.daml_libraries_version
  val daml_language_versions = metabuild.BuildInfo.daml_language_versions
  val daml_compiler_version = metabuild.BuildInfo.daml_compiler_version
  val use_custom_daml_version = metabuild.BuildInfo.use_custom_daml_version

  lazy val osClassifier: String =
    if (sys.props("os.name").contains("Mac")) "osx" else sys.props("os.name").toLowerCase

  // Used to exclude slf4j 1.x and logback 1.2.x transitive dependencies, since they're incompatible
  // with slf4j 2.x and logback 1.4.x currently in use.
  lazy val incompatibleLogging: Array[ExclusionRule] =
    Array(ExclusionRule("org.slf4j"), ExclusionRule("ch.qos.logback"))

  lazy val scala_version = "2.13.10"
  lazy val scala_version_short = "2.13"

  // TODO(#10617) We have cloned akka's BroadcastHub implementation in community/lib/akka/src/main/scala/akka/stream/scaladsl/BroadcastHub.scala
  //  When updating akka, make sure to update the clone as well, including the tests in community/lib/akka/src/main/scala/akka
  lazy val akka_version = "2.6.18"
  lazy val akka_http_version = "10.2.8"
  lazy val grpc_version = "1.44.0"
  lazy val logback_version = "1.4.5"
  lazy val slf4j_version = "2.0.6"
  lazy val log4j_version = "2.17.0"
  lazy val ammonite_version = "2.5.5"
  lazy val pprint_version = "0.8.1"
  // if you update the slick version, please also update our forked code in common/slick.util.*
  lazy val slick_version = "3.3.3"
  lazy val bouncy_castle_version = "1.70"

  lazy val pureconfig_version = "0.14.0"

  lazy val circe_version = "0.14.2"

  lazy val scalatest_version = "3.2.9"

  lazy val aws_kms_version = "2.17.187"
  lazy val netty_version = "4.1.72.Final"

  lazy val reflections = "org.reflections" % "reflections" % "0.9.12"
  lazy val pureconfig = "com.github.pureconfig" %% "pureconfig" % pureconfig_version
  lazy val pureconfig_cats = "com.github.pureconfig" %% "pureconfig-cats" % pureconfig_version

  lazy val scala_collection_contrib =
    "org.scala-lang.modules" %% "scala-collection-contrib" % "0.2.2"
  lazy val scala_reflect = "org.scala-lang" % "scala-reflect" % scala_version
  lazy val scala_compiler = "org.scala-lang" % "scala-compiler" % scala_version
  lazy val shapeless = "com.chuusai" %% "shapeless" % "2.3.6"

  lazy val monocle_version = "3.1.0"
  lazy val monocle_core = "dev.optics" %% "monocle-core" % monocle_version
  lazy val monocle_macro = "dev.optics" %% "monocle-macro" % monocle_version

  // ammonite requires the full scala version including patch number
  lazy val ammonite = "com.lihaoyi" % "ammonite" % ammonite_version cross CrossVersion.full
  lazy val pprint = "com.lihaoyi" %% "pprint" % pprint_version

  lazy val h2 = "com.h2database" % "h2" % "2.1.210"
  lazy val postgres = "org.postgresql" % "postgresql" % "42.3.8"
  lazy val flyway = "org.flywaydb" % "flyway-core" % "9.15.2"
  lazy val oracle = "com.oracle.database.jdbc" % "ojdbc8" % "19.18.0.0"

  // Picked up automatically by the scalapb compiler. Contains common dependencies such as protocol buffers like google/protobuf/timestamp.proto
  lazy val scalapb_runtime =
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
  lazy val scalapb_runtime_grpc =
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion

  lazy val bouncycastle_bcprov_jdk15on =
    "org.bouncycastle" % "bcprov-jdk15on" % bouncy_castle_version
  lazy val bouncycastle_bcpkix_jdk15on =
    "org.bouncycastle" % "bcpkix-jdk15on" % bouncy_castle_version

  lazy val javax_annotations = "javax.annotation" % "javax.annotation-api" % "1.3.2"

  lazy val grpc_protobuf = "io.grpc" % "grpc-protobuf" % grpc_version
  lazy val grpc_netty = "io.grpc" % "grpc-netty" % grpc_version

  // pick the version of boring ssl and netty native from this table: https://github.com/grpc/grpc-java/blob/master/SECURITY.md#netty
  // required for ALPN (which is required for TLS+HTTP/2) when running on Java 8. JSSE will be used on Java 9+.
  lazy val netty_boring_ssl = "io.netty" % "netty-tcnative-boringssl-static" % "2.0.46.Final"
  lazy val netty_handler = "io.netty" % "netty-handler" % netty_version
  lazy val netty_native =
    "io.netty" % "netty-transport-native-epoll" % netty_version classifier "linux-x86_64" classifier "linux-aarch_64"
  lazy val netty_native_s390 =
    "io.netty" % "netty-transport-native-epoll-s390x" % "4.1.77.Final-redhat-00001" classifier "linux-s390_64"
  lazy val grpc_stub = "io.grpc" % "grpc-stub" % grpc_version
  lazy val grpc_services = "io.grpc" % "grpc-services" % grpc_version
  lazy val grpc_api = "io.grpc" % "grpc-api" % grpc_version

  lazy val scopt = "com.github.scopt" %% "scopt" % "4.1.0"

  lazy val akka_actor_typed = "com.typesafe.akka" %% "akka-actor-typed" % akka_version
  lazy val akka_actor_testkit_typed =
    "com.typesafe.akka" %% "akka-actor-testkit-typed" % akka_version
  lazy val akka_stream = "com.typesafe.akka" %% "akka-stream" % akka_version
  lazy val akka_stream_testkit = "com.typesafe.akka" %% "akka-stream-testkit" % akka_version
  lazy val akka_slf4j =
    "com.typesafe.akka" %% "akka-slf4j" % akka_version excludeAll (incompatibleLogging: _*)
  lazy val akka_http = "com.typesafe.akka" %% "akka-http" % akka_http_version
  lazy val akka_http_testkit = "com.typesafe.akka" %% "akka-http-testkit" % akka_http_version

  lazy val scala_logging =
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5" excludeAll (incompatibleLogging: _*)
  lazy val scalacheck = "org.scalacheck" %% "scalacheck" % "1.15.4"
  lazy val scalatest = "org.scalatest" %% "scalatest" % scalatest_version
  lazy val scalatestScalacheck =
    "org.scalatestplus" %% "scalacheck-1-15" % (scalatest_version + ".0")
  lazy val mockito_scala = "org.mockito" %% "mockito-scala" % "1.16.3"
  lazy val scalatestMockito = "org.scalatestplus" %% "mockito-3-4" % (scalatest_version + ".0")

  // TODO(#10852) Gerolf says that there is little value behind the scala wrapper. so let's drop it
  //              and use caffeine directly
  lazy val scaffeine = "com.github.blemale" %% "scaffeine" % "5.2.1"

  lazy val slf4j_api = "org.slf4j" % "slf4j-api" % slf4j_version
  lazy val jul_to_slf4j = "org.slf4j" % "jul-to-slf4j" % slf4j_version
  lazy val logback_classic = "ch.qos.logback" % "logback-classic" % logback_version

  lazy val logback_core = "ch.qos.logback" % "logback-core" % logback_version

  lazy val log4j_core = "org.apache.logging.log4j" % "log4j-core" % log4j_version
  lazy val log4j_api = "org.apache.logging.log4j" % "log4j-api" % log4j_version

  // used for condition evaluation in logback
  lazy val janino = "org.codehaus.janino" % "janino" % "3.1.4"
  lazy val logstash = "net.logstash.logback" % "logstash-logback-encoder" % "6.6"

  lazy val cats = "org.typelevel" %% "cats-core" % "2.8.0"
  lazy val cats_law = "org.typelevel" %% "cats-laws" % "2.8.0"
  lazy val cats_scalacheck = "io.chrisdavenport" %% "cats-scalacheck" % "0.3.2"

  lazy val chimney = "io.scalaland" %% "chimney" % "0.6.1"

  // TODO(#10852) yet another json lbirary
  lazy val circe_core = "io.circe" %% "circe-core" % circe_version
  lazy val circe_generic = "io.circe" %% "circe-generic" % circe_version
  lazy val circe_generic_extras = "io.circe" %% "circe-generic-extras" % circe_version
  lazy val circe_parser = "io.circe" %% "circe-parser" % circe_version
  lazy val circe_yaml = "io.circe" %% "circe-yaml" % circe_version

  lazy val tink = "com.google.crypto.tink" % "tink" % "1.3.0" excludeAll (
    ExclusionRule(
      organization = "com.google.guava",
      name = "guava-jdk5",
    ),
    ExclusionRule(
      organization = "com.amazonaws",
      name = "aws-java-sdk-kms",
    ),
  )

  lazy val dropwizard_metrics_core = "io.dropwizard.metrics" % "metrics-core" % "4.1.33"
  lazy val dropwizard_metrics_jmx = "io.dropwizard.metrics" % "metrics-jmx" % "4.1.33"
  lazy val dropwizard_metrics_jvm = "io.dropwizard.metrics" % "metrics-jvm" % "4.1.33"
  lazy val dropwizard_metrics_graphite = "io.dropwizard.metrics" % "metrics-graphite" % "4.1.33"

  lazy val prometheus_dropwizard = "io.prometheus" % "simpleclient_dropwizard" % "0.14.1"
  lazy val prometheus_httpserver = "io.prometheus" % "simpleclient_httpserver" % "0.14.1"
  lazy val prometheus_hotspot = "io.prometheus" % "simpleclient_hotspot" % "0.14.1"

  lazy val opentelemetry_version = "1.12.0"
  lazy val opentelemetry_api = "io.opentelemetry" % "opentelemetry-api" % opentelemetry_version
  lazy val opentelemetry_sdk = "io.opentelemetry" % "opentelemetry-sdk" % opentelemetry_version
  lazy val opentelemetry_sdk_testing =
    "io.opentelemetry" % "opentelemetry-sdk-testing" % opentelemetry_version
  lazy val opentelemetry_sdk_autoconfigure =
    "io.opentelemetry" % "opentelemetry-sdk-extension-autoconfigure" % s"$opentelemetry_version-alpha"
  lazy val opentelemetry_zipkin =
    "io.opentelemetry" % "opentelemetry-exporter-zipkin" % opentelemetry_version
  lazy val opentelemetry_jaeger =
    "io.opentelemetry" % "opentelemetry-exporter-jaeger" % opentelemetry_version

  lazy val opentelemetry_instrumentation_grpc =
    "io.opentelemetry.instrumentation" % "opentelemetry-grpc-1.6" % s"$opentelemetry_version-alpha"
  lazy val opentelemetry_instrumentation_runtime_metrics =
    "io.opentelemetry.instrumentation" % "opentelemetry-runtime-metrics" % s"$opentelemetry_version-alpha"

  lazy val better_files = "com.github.pathikrit" %% "better-files" % "3.9.1"

  // TODO(#10852) one database library, not two
  lazy val slick =
    "com.typesafe.slick" %% "slick" % slick_version excludeAll (incompatibleLogging: _*)
  lazy val slick_hikaricp = "com.typesafe.slick" %% "slick-hikaricp" % slick_version

  lazy val testcontainers_version = "1.15.1"
  lazy val testcontainers = "org.testcontainers" % "testcontainers" % testcontainers_version
  lazy val testcontainers_postgresql = "org.testcontainers" % "postgresql" % testcontainers_version

  lazy val sttp_version = "3.1.7"
  lazy val sttp = "com.softwaremill.sttp.client3" %% "core" % sttp_version
  lazy val sttp_okhttp = "com.softwaremill.sttp.client3" %% "okhttp-backend" % sttp_version
  lazy val sttp_circe = "com.softwaremill.sttp.client3" %% "circe" % sttp_version
  lazy val sttp_slf4j = "com.softwaremill.sttp.client3" %% "slf4j-backend" % sttp_version

  // demo dependencies (you also need to update demo.sc)
  lazy val scalafx = "org.scalafx" %% "scalafx" % "17.0.1-R26"
  // TODO(i8460) Don't upgrade until https://github.com/sbt/sbt/issues/6564 is fixed
  lazy val javafx_all = Seq("controls", "base", "fxml", "media", "web", "graphics").map { x =>
    "org.openjfx" % s"javafx-$x" % "17-ea+8"
  }

  lazy val toxiproxy_java = "eu.rekawek.toxiproxy" % "toxiproxy-java" % "2.1.7"

  lazy val fabric_sdk = "org.hyperledger.fabric-sdk-java" % "fabric-sdk-java" % "2.2.13"
  lazy val snakeyaml = "org.yaml" % "snakeyaml" % "2.0"

  lazy val web3j = "org.web3j" % "core" % "4.9.7"

  lazy val concurrency_limits =
    "com.netflix.concurrency-limits" % "concurrency-limits-grpc" % "0.3.6"

  lazy val wartremover_dep =
    "org.wartremover" % "wartremover" % wartremover.Wart.PluginVersion cross CrossVersion.full

  lazy val scala_csv = "com.github.tototoshi" %% "scala-csv" % "1.3.10"

  // AWS SDK for Java API to encrypt/decrypt keys using KMS
  lazy val aws_kms = "software.amazon.awssdk" % "kms" % aws_kms_version

  lazy val damlDependencyMap = {
    import io.circe._, io.circe.parser._, io.circe.generic.auto._, io.circe.syntax._
    import better.files._

    case class Dependencies(dependency_tree: DependencyTree)
    case class DependencyTree(dependencies: List[Dependency])
    case class Dependency(coord: String)

    val deps = decode[Dependencies](
      file"daml/maven_install_2.13.json".contentAsString
    ).valueOr { err =>
      throw new RuntimeException(s"Failed to parse daml repo maven json file: $err")
    }

    deps.dependency_tree.dependencies.mapFilter { dep =>
      dep.coord.split(":") match {
        case Array(org, artifact, version) =>
          Some((org, artifact) -> (org % artifact % version))
        case _ => None
      }
    }.toMap
  }

  def damlDependency(org: String, artifact: String): ModuleID = {
    damlDependencyMap
      .get((org, artifact))
      .orElse(damlDependencyMap.get((org, s"${artifact}_2.13")))
      .getOrElse(throw new RuntimeException(s"Unknown daml dependency: $org, $artifact"))
  }

  // TODO(i10677): Pull in dependencies from daml submodule
  lazy val daml_script_runner = "com.daml" %% "daml-script-runner" % daml_libraries_version

  // daml repo dependencies
  // TODO(#10852) scalaz or cats. let's pick one.
  lazy val scalaz_core = damlDependency("org.scalaz", "scalaz-core")
  lazy val scalaz_scalacheck = damlDependency("org.scalaz", "scalaz-scalacheck-binding")
  lazy val fasterjackson_core = damlDependency("com.fasterxml.jackson.core", "jackson-core")
  // TODO(#10852) yet another json library
  lazy val spray = damlDependency("io.spray", "spray-json")
  lazy val google_protobuf_java = damlDependency("com.google.protobuf", "protobuf-java")
  lazy val protobuf_version = google_protobuf_java.revision
  // To override 3.19.2 from the daml repo's maven_install_2.13.json
  lazy val google_protobuf_java_util =
    "com.google.protobuf" % "protobuf-java-util" % protobuf_version

  // version depends actually on scalapb
  lazy val google_common_protos =
    damlDependency("com.google.api.grpc", "proto-google-common-protos")

  lazy val apache_commons_text = damlDependency("org.apache.commons", "commons-text")
  lazy val typelevel_paiges = damlDependency("org.typelevel", "paiges-core")
  lazy val commons_io = damlDependency("commons-io", "commons-io")
  lazy val commons_codec = damlDependency("commons-codec", "commons-codec")
  lazy val lihaoyi_sourcecode = damlDependency("com.lihaoyi", "sourcecode")
  lazy val totososhi = damlDependency("com.github.tototoshi", "scala-csv")
  lazy val auth0_java = damlDependency("com.auth0", "java-jwt")
  lazy val auth0_jwks = damlDependency("com.auth0", "jwks-rsa")
  lazy val guava = damlDependency("com.google.guava", "guava")
  // TODO(#10852) one database library, not two
  lazy val anorm = damlDependency("org.playframework.anorm", "anorm")
  lazy val anorm_akka = damlDependency("org.playframework.anorm", "anorm-akka")
  lazy val scalapb_json4s = damlDependency("com.thesamet.scalapb", "scalapb-json4s")
  lazy val opentelemetry_prometheus =
    damlDependency("io.opentelemetry", "opentelemetry-exporter-prometheus")
  // it should be kept up-to-date with the scaffeine version to avoid incompatibilities
  lazy val caffeine = damlDependency("com.github.ben-manes.caffeine", "caffeine")
  lazy val hikaricp = damlDependency("com.zaxxer", "HikariCP")

  lazy val awaitility = "org.awaitility" % "awaitility" % "4.2.0"

}
