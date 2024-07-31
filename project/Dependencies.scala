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

  lazy val scala_version = "2.13.11"
  lazy val scala_version_short = "2.13"

  lazy val pekko_version = "1.0.1"
  // TODO(#10617) We have cloned pekko's BroadcastHub implementation in community/lib/pekko/src/main/scala/pekko/stream/scaladsl/BroadcastHub.scala
  //  When updating pekko, make sure to update the clone as well, including the tests in community/lib/pekko/src/main/scala/pekko
  lazy val pekko_http_version = "1.0.0"
  lazy val ammonite_version = "2.5.9"
  lazy val awaitility_version = "4.2.0"
  lazy val aws_version = "2.22.3"
  lazy val better_files_version = "3.9.1"
  lazy val bouncy_castle_version = "1.70"
  lazy val cats_law_version = "2.9.0"
  lazy val cats_scalacheck_version = "0.3.2"
  lazy val cats_version = "2.9.0"
  lazy val checkerFramework_version = "3.28.0"
  lazy val chimney_version = "0.6.1"
  lazy val circe_version = "0.14.2"
  lazy val concurrency_limits_version = "0.3.6"
  lazy val dropwizard_version = "4.2.25"
  lazy val fabric_sdk_version = "2.2.13"
  lazy val flyway_version = "9.15.2"
  lazy val gcp_kms_version = "2.33.0"
  lazy val h2_version = "2.1.210"
  lazy val janino_version = "3.1.4"
  // TODO(i8460) Don't upgrade until https://github.com/sbt/sbt/issues/6564 is fixed
  lazy val javafx_all_version = "17-ea+8"
  lazy val javax_annotations_version = "1.3.2"
  lazy val log4j_version = "2.17.0"
  lazy val logback_version = "1.4.5"
  lazy val logstash_version = "6.6"
  lazy val magnolia_version = "1.1.3"
  lazy val magnolifyScalacheck_version = "0.6.2"
  lazy val magnolifyShared_version = "0.6.2"
  lazy val mockito_scala_version = "1.16.3"
  lazy val monocle_version = "3.2.0"
  // pick the version of boring ssl and netty native from this table: https://github.com/grpc/grpc-java/blob/master/SECURITY.md#netty
  // required for ALPN (which is required for TLS+HTTP/2) when running on Java 8. JSSE will be used on Java 9+.
  lazy val grpc_version = "1.60.0"
  lazy val netty_boring_ssl_version = "2.0.61.Final"
  lazy val netty_version = "4.1.108.Final"
  lazy val opentelemetry_instrumentation_grpc_version = s"$opentelemetry_version-alpha"
  lazy val opentelemetry_instrumentation_runtime_metrics_version = s"$opentelemetry_version-alpha"
  lazy val opentelemetry_proto_version = "1.7.1-alpha"
  lazy val opentelemetry_sdk_autoconfigure_version = s"$opentelemetry_version-alpha"
  lazy val opentelemetry_version = "1.12.0"
  lazy val oracle_version = "19.18.0.0"
  lazy val postgres_version = "42.7.3"
  lazy val pprint_version = "0.8.1"
  lazy val prometheus_version = "0.14.1"
  lazy val pureconfig_version = "0.14.0"
  lazy val reflections_version = "0.10.2"
  lazy val scaffeine_version = "5.2.1"
  lazy val scala_collections_contrib_version = "0.2.2"
  lazy val scala_parser_combinators_version = "1.1.2"
  lazy val scala_csv_version = "1.3.10"
  lazy val scala_logging_version = "3.9.5"
  lazy val scalacheck_version = "1.15.4"
  lazy val scalafx_version = "17.0.1-R26"
  lazy val scalafx_all_version = "17-ea+8"
  lazy val scalatest_version = "3.2.11"
  lazy val scopt_version = "4.1.0"
  lazy val shapeless_version = "2.3.6"
  lazy val slf4j_version = "2.0.6"
  // if you update the slick version, please also update our forked code in common/slick.util.*
  lazy val slick_version = "3.3.3"
  lazy val snakeyaml_version = "2.0"
  lazy val spray_json_derived_codecs_version = "2.3.10"
  lazy val sttp_version = "3.8.16"

  // Note that later versions of testcontainers (e.g. 1.19.7 give problems with Oracle when running the data continuity tests"
  lazy val testcontainers_version = "1.15.1"

  lazy val tink_version = "1.3.0"
  lazy val toxiproxy_java_version = "2.1.7"
  lazy val web3j_version = "4.9.7"

  lazy val reflections = "org.reflections" % "reflections" % reflections_version
  lazy val pureconfig_core =
    "com.github.pureconfig" %% "pureconfig-core" % pureconfig_version exclude ("com.chuusai", s"shapeless_$scala_version_short")
  lazy val pureconfig_cats = "com.github.pureconfig" %% "pureconfig-cats" % pureconfig_version
  lazy val pureconfig_generic =
    "com.github.pureconfig" %% "pureconfig-generic" % pureconfig_version exclude ("com.chuusai", s"shapeless_$scala_version_short")
  lazy val pureconfig_macros = "com.github.pureconfig" %% "pureconfig-macros" % pureconfig_version

  lazy val scala_collection_contrib =
    "org.scala-lang.modules" %% "scala-collection-contrib" % scala_collections_contrib_version
  lazy val scala_parser_combinators =
    "org.scala-lang.modules" %% "scala-parser-combinators" % scala_parser_combinators_version
  lazy val scala_reflect = "org.scala-lang" % "scala-reflect" % scala_version
  lazy val scala_compiler = "org.scala-lang" % "scala-compiler" % scala_version
  lazy val shapeless = "com.chuusai" %% "shapeless" % shapeless_version

  lazy val monocle_core = "dev.optics" %% "monocle-core" % monocle_version
  lazy val monocle_macro = "dev.optics" %% "monocle-macro" % monocle_version

  // ammonite requires the full scala version including patch number
  lazy val ammonite = "com.lihaoyi" % "ammonite" % ammonite_version cross CrossVersion.full
  lazy val pprint = "com.lihaoyi" %% "pprint" % pprint_version

  lazy val h2 = "com.h2database" % "h2" % h2_version
  lazy val postgres = "org.postgresql" % "postgresql" % postgres_version
  lazy val flyway = "org.flywaydb" % "flyway-core" % flyway_version
  lazy val oracle = "com.oracle.database.jdbc" % "ojdbc8" % oracle_version

  // Picked up automatically by the scalapb compiler. Contains common dependencies such as protocol buffers like google/protobuf/timestamp.proto
  lazy val scalapb_runtime =
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
  lazy val scalapb_runtime_grpc =
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion

  lazy val bouncycastle_bcprov_jdk15on =
    "org.bouncycastle" % "bcprov-jdk15on" % bouncy_castle_version
  lazy val bouncycastle_bcpkix_jdk15on =
    "org.bouncycastle" % "bcpkix-jdk15on" % bouncy_castle_version

  lazy val javax_annotations =
    "javax.annotation" % "javax.annotation-api" % javax_annotations_version

  lazy val grpc_protobuf = "io.grpc" % "grpc-protobuf" % grpc_version
  lazy val grpc_netty = "io.grpc" % "grpc-netty" % grpc_version
  lazy val grpc_netty_shaded = "io.grpc" % "grpc-netty-shaded" % grpc_version

  lazy val netty_boring_ssl =
    "io.netty" % "netty-tcnative-boringssl-static" % netty_boring_ssl_version
  lazy val netty_handler = "io.netty" % "netty-handler" % netty_version
  lazy val netty_native =
    "io.netty" % "netty-transport-native-epoll" % netty_version classifier "linux-x86_64" classifier "linux-aarch_64"
  lazy val grpc_stub = "io.grpc" % "grpc-stub" % grpc_version
  lazy val grpc_services = "io.grpc" % "grpc-services" % grpc_version
  lazy val grpc_api = "io.grpc" % "grpc-api" % grpc_version
  lazy val grpc_inprocess = "io.grpc" % "grpc-inprocess" % grpc_version

  lazy val scopt = "com.github.scopt" %% "scopt" % scopt_version

  lazy val pekko_actor_typed = "org.apache.pekko" %% "pekko-actor-typed" % pekko_version
  lazy val pekko_actor_testkit_typed =
    "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekko_version
  lazy val pekko_stream = "org.apache.pekko" %% "pekko-stream" % pekko_version
  lazy val pekko_stream_testkit = "org.apache.pekko" %% "pekko-stream-testkit" % pekko_version
  lazy val pekko_slf4j =
    "org.apache.pekko" %% "pekko-slf4j" % pekko_version excludeAll (incompatibleLogging: _*)
  lazy val pekko_http = "org.apache.pekko" %% "pekko-http" % pekko_http_version
  lazy val pekko_http_core = "org.apache.pekko" %% "pekko-http-core" % pekko_http_version
  lazy val pekko_http_testkit = "org.apache.pekko" %% "pekko-http-testkit" % pekko_http_version

  lazy val spray_json_derived_codecs =
    "io.github.paoloboni" %% "spray-json-derived-codecs" % spray_json_derived_codecs_version exclude ("com.chuusai", s"shapeless_$scala_version_short")

  lazy val scala_logging =
    "com.typesafe.scala-logging" %% "scala-logging" % scala_logging_version excludeAll (incompatibleLogging: _*)
  lazy val scalacheck = "org.scalacheck" %% "scalacheck" % scalacheck_version
  lazy val scalatest = "org.scalatest" %% "scalatest" % scalatest_version
  lazy val scalatestScalacheck =
    "org.scalatestplus" %% "scalacheck-1-15" % (scalatest_version + ".0")
  lazy val mockito_scala = "org.mockito" %% "mockito-scala" % mockito_scala_version
  lazy val scalatestMockito = "org.scalatestplus" %% "mockito-3-4" % ("3.2.10.0")

  /*
  "org.junit.jupiter:junit-jupiter-api:5.9.2",
  "org.junit.jupiter:junit-jupiter-engine:5.9.2",
  "org.junit.platform:junit-platform-engine:1.9.2",
  "org.junit.platform:junit-platform-runner:1.9.2",
   */

  lazy val junit_jupiter_api = "org.junit.jupiter" % "junit-jupiter-api" % "5.9.2"
  lazy val junit_jupiter_engine = "org.junit.jupiter" % "junit-jupiter-engine" % "5.9.2"
  lazy val junit_platform_runner = "org.junit.platform" % "junit-platform-runner" % "1.9.2"
  lazy val jupiter_interface = "net.aichler" % "jupiter-interface" % "0.9.0"

  lazy val checkerFramework = "org.checkerframework" % "checker-qual" % checkerFramework_version

  // TODO(#10852) Gerolf says that there is little value behind the scala wrapper. so let's drop it
  //              and use caffeine directly
  lazy val scaffeine = "com.github.blemale" %% "scaffeine" % scaffeine_version

  lazy val slf4j_api = "org.slf4j" % "slf4j-api" % slf4j_version
  lazy val jul_to_slf4j = "org.slf4j" % "jul-to-slf4j" % slf4j_version
  lazy val logback_classic = "ch.qos.logback" % "logback-classic" % logback_version

  lazy val logback_core = "ch.qos.logback" % "logback-core" % logback_version

  lazy val log4j_core = "org.apache.logging.log4j" % "log4j-core" % log4j_version
  lazy val log4j_api = "org.apache.logging.log4j" % "log4j-api" % log4j_version

  // used for condition evaluation in logback
  lazy val janino = "org.codehaus.janino" % "janino" % janino_version
  lazy val logstash = "net.logstash.logback" % "logstash-logback-encoder" % logstash_version

  lazy val cats = "org.typelevel" %% "cats-core" % cats_version
  lazy val cats_law = "org.typelevel" %% "cats-laws" % cats_law_version
  lazy val cats_scalacheck = "io.chrisdavenport" %% "cats-scalacheck" % cats_scalacheck_version

  lazy val chimney = "io.scalaland" %% "chimney" % chimney_version

  lazy val magnolia = "com.softwaremill.magnolia1_2" %% "magnolia" % magnolia_version
  lazy val magnolifyShared =
    "com.spotify" % "magnolify-shared_2.13" % magnolifyShared_version exclude ("com.chuusai", s"shapeless_$scala_version_short")
  lazy val magnolifyScalacheck =
    "com.spotify" % "magnolify-scalacheck_2.13" % magnolifyScalacheck_version exclude ("com.chuusai", s"shapeless_$scala_version_short")

  // TODO(#10852) yet another json library
  lazy val circe_core = "io.circe" %% "circe-core" % circe_version
  lazy val circe_generic =
    "io.circe" %% "circe-generic" % circe_version exclude ("com.chuusai", s"shapeless_$scala_version_short")
  lazy val circe_generic_extras =
    "io.circe" %% "circe-generic-extras" % circe_version exclude ("com.chuusai", s"shapeless_$scala_version_short")
  lazy val circe_parser = "io.circe" %% "circe-parser" % circe_version
  lazy val circe_yaml = "io.circe" %% "circe-yaml" % circe_version

  lazy val tink = "com.google.crypto.tink" % "tink" % tink_version excludeAll (
    ExclusionRule(
      organization = "com.google.guava",
      name = "guava-jdk5",
    ),
    ExclusionRule(
      organization = "com.amazonaws",
      name = "aws-java-sdk-kms",
    ),
  )

  lazy val dropwizard_metrics_core = "io.dropwizard.metrics" % "metrics-core" % dropwizard_version
  lazy val dropwizard_metrics_jmx = "io.dropwizard.metrics" % "metrics-jmx" % dropwizard_version
  lazy val dropwizard_metrics_jvm = "io.dropwizard.metrics" % "metrics-jvm" % dropwizard_version
  lazy val dropwizard_metrics_graphite =
    "io.dropwizard.metrics" % "metrics-graphite" % dropwizard_version

  lazy val prometheus_dropwizard = "io.prometheus" % "simpleclient_dropwizard" % prometheus_version
  lazy val prometheus_httpserver = "io.prometheus" % "simpleclient_httpserver" % prometheus_version
  lazy val prometheus_hotspot = "io.prometheus" % "simpleclient_hotspot" % prometheus_version

  lazy val opentelemetry_api = "io.opentelemetry" % "opentelemetry-api" % opentelemetry_version
  lazy val opentelemetry_sdk = "io.opentelemetry" % "opentelemetry-sdk" % opentelemetry_version
  lazy val opentelemetry_sdk_testing =
    "io.opentelemetry" % "opentelemetry-sdk-testing" % opentelemetry_version
  lazy val opentelemetry_sdk_autoconfigure =
    "io.opentelemetry" % "opentelemetry-sdk-extension-autoconfigure" % opentelemetry_sdk_autoconfigure_version
  lazy val opentelemetry_zipkin =
    "io.opentelemetry" % "opentelemetry-exporter-zipkin" % opentelemetry_version
  lazy val opentelemetry_jaeger =
    "io.opentelemetry" % "opentelemetry-exporter-jaeger" % opentelemetry_version
  lazy val opentelemetry_otlp =
    "io.opentelemetry" % "opentelemetry-exporter-otlp-trace" % opentelemetry_version
  lazy val opentelemetry_proto =
    "io.opentelemetry" % "opentelemetry-proto" % opentelemetry_proto_version

  lazy val opentelemetry_instrumentation_grpc =
    "io.opentelemetry.instrumentation" % "opentelemetry-grpc-1.6" % opentelemetry_instrumentation_grpc_version
  lazy val opentelemetry_instrumentation_runtime_metrics =
    "io.opentelemetry.instrumentation" % "opentelemetry-runtime-metrics" % opentelemetry_instrumentation_runtime_metrics_version

  lazy val better_files = "com.github.pathikrit" %% "better-files" % better_files_version

  // TODO(#10852) one database library, not two
  lazy val slick =
    "com.typesafe.slick" %% "slick" % slick_version excludeAll (incompatibleLogging: _*)
  lazy val slick_hikaricp = "com.typesafe.slick" %% "slick-hikaricp" % slick_version

  lazy val testcontainers = "org.testcontainers" % "testcontainers" % testcontainers_version
  lazy val testcontainers_postgresql = "org.testcontainers" % "postgresql" % testcontainers_version

  lazy val sttp = "com.softwaremill.sttp.client3" %% "core" % sttp_version
  lazy val sttp_circe = "com.softwaremill.sttp.client3" %% "circe" % sttp_version
  lazy val sttp_slf4j = "com.softwaremill.sttp.client3" %% "slf4j-backend" % sttp_version

  // demo dependencies (you also need to update demo.sc)
  lazy val scalafx = "org.scalafx" %% "scalafx" % scalafx_version
  lazy val javafx_all = Seq("controls", "base", "fxml", "media", "web", "graphics").map { x =>
    "org.openjfx" % s"javafx-$x" % scalafx_all_version
  }

  lazy val toxiproxy_java = "eu.rekawek.toxiproxy" % "toxiproxy-java" % toxiproxy_java_version

  lazy val fabric_sdk = "org.hyperledger.fabric-sdk-java" % "fabric-sdk-java" % fabric_sdk_version
  lazy val snakeyaml = "org.yaml" % "snakeyaml" % snakeyaml_version

  lazy val web3j = "org.web3j" % "core" % web3j_version

  lazy val concurrency_limits =
    "com.netflix.concurrency-limits" % "concurrency-limits-grpc" % concurrency_limits_version

  lazy val wartremover_dep =
    "org.wartremover" % "wartremover" % wartremover.Wart.PluginVersion cross CrossVersion.full

  lazy val scala_csv = "com.github.tototoshi" %% "scala-csv" % scala_csv_version

  // AWS SDK for Java API to encrypt/decrypt keys using AWS KMS
  lazy val aws_kms = "software.amazon.awssdk" % "kms" % aws_version
  lazy val aws_sts = "software.amazon.awssdk" % "sts" % aws_version

  // GCP SDK for Java API to encrypt/decrypt keys using GCP KMS
  lazy val gcp_kms = "com.google.cloud" % "google-cloud-kms" % gcp_kms_version

  lazy val awaitility = "org.awaitility" % "awaitility" % awaitility_version

  lazy val damlDependencyMap = {
    import io.circe._, io.circe.parser._, io.circe.generic.auto._, io.circe.syntax._
    import better.files._

    case class Dependencies(dependency_tree: DependencyTree)
    case class DependencyTree(dependencies: List[Dependency])
    case class Dependency(coord: String)

    val deps = decode[Dependencies](
      file"daml/sdk/maven_install_2.13.json".contentAsString
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

  def damlDependency(org: String, artifact: String): ModuleID =
    damlDependencyMap
      .get((org, artifact))
      .orElse(damlDependencyMap.get((org, s"${artifact}_2.13")))
      .getOrElse(throw new RuntimeException(s"Unknown daml dependency: $org, $artifact"))

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
  lazy val scalapb_json4s = damlDependency("com.thesamet.scalapb", "scalapb-json4s")
  lazy val opentelemetry_prometheus =
    damlDependency("io.opentelemetry", "opentelemetry-exporter-prometheus")
  // it should be kept up-to-date with the scaffeine version to avoid incompatibilities
  lazy val caffeine = damlDependency("com.github.ben-manes.caffeine", "caffeine")
  lazy val hikaricp = damlDependency("com.zaxxer", "HikariCP")
}
