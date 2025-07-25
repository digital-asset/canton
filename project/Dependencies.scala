import cats.syntax.either.*
import cats.syntax.functorFilter.*
import sbt.{io as _, *}

import scala.util.chaining.scalaUtilChainingOps

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

  lazy val scala_version = "2.13.16"
  lazy val scala_version_short = "2.13"

  // TODO(#10617) We have cloned pekko's BroadcastHub implementation in community/lib/pekko/src/main/scala/pekko/stream/scaladsl/BroadcastHub.scala
  //  The changes from the clone have been upstreamed in https://github.com/apache/pekko/pull/1841.
  //  When updating pekko, check whether the update includes the above fix (probably only in 1.2.x)
  //  If it is included, remove the clone. Otherwise, make sure to update the clone as well,
  //  including the tests in community/lib/pekko/src/main/scala/pekko
  lazy val pekko_version = "1.1.2"
  lazy val pekko_http_version = "1.1.0"
  lazy val ammonite_version = "3.0.1"
  lazy val apispec_version = "0.11.7"
  lazy val awaitility_version = "4.2.0"
  lazy val aws_version = "2.29.5"
  lazy val better_files_version = "3.9.2"
  lazy val bouncy_castle_version = "1.70"
  lazy val cats_law_version = "2.9.0"
  lazy val cats_scalacheck_version = "0.3.2"
  lazy val cats_version = "2.9.0"
  lazy val checkerFramework_version = "3.28.0"
  lazy val chimney_version = "1.4.0"
  lazy val circe_version = "0.14.2"
  lazy val circe_yaml_version = "1.15.0" // added to override snakeYaml vulnerability
  lazy val dropwizard_version = "4.1.33"
  lazy val flyway_version = "10.22.0"
  lazy val gcp_kms_version = "2.55.0"
  lazy val gson_version = "2.9.1"
  lazy val gson_fire_version = "1.9.0"
  lazy val h2_version = "2.2.224"
  lazy val janino_version = "3.1.12"
  lazy val javax_annotations_version = "1.3.2"
  lazy val jackson_databind_nullable_version = "0.2.6"
  lazy val jakarta_annotation_api_version = "1.3.5"
  lazy val log4j_version = "2.23.1"
  lazy val logback_version = "1.5.3"
  lazy val logstash_version = "6.6"
  lazy val magnolia_version = "1.1.3"
  lazy val magnolifyScalacheck_version = "0.6.2"
  lazy val magnolifyShared_version = "0.6.2"
  lazy val mockito_scala_version = "1.16.3"
  lazy val monocle_version = "3.2.0"
  lazy val munit_version = "0.7.26"
  // pick the version of boring ssl and netty native from this table: https://github.com/grpc/grpc-java/blob/master/SECURITY.md#netty
  // required for ALPN (which is required for TLS+HTTP/2) when running on Java 8. JSSE will be used on Java 9+.
  lazy val grpc_version = "1.67.1"
  lazy val netty_boring_ssl_version = "2.0.65.Final"
  lazy val netty_version = "4.1.110.Final"

  lazy val pekko_http_backend_version = "3.9.0"
  lazy val protostuff_version = "3.1.40"
  lazy val postgres_version = "42.7.3"
  lazy val pprint_version = "0.8.1"
  lazy val protoc_gen_doc_version = "1.5.1"
  lazy val pureconfig_version = "0.14.0"
  lazy val reflections_version = "0.10.2"
  lazy val scaffeine_version = "5.2.1"
  lazy val scala_collections_contrib_version = "0.2.2"
  lazy val scala_csv_version = "1.3.10"
  lazy val scala_logging_version = "3.9.5"
  lazy val scalacheck_version = "1.15.4"
  lazy val scalafx_version = "17.0.1-R26"
  lazy val scalafx_all_version = "17-ea+8"
  lazy val scalatest_version = "3.2.19"
  lazy val scalaz_version = "7.2.33"
  lazy val scopt_version = "4.1.0"
  lazy val shapeless_version = "2.3.7"
  lazy val slf4j_version = "2.0.6"

  // if you update the slick version, please also update our forked code in community/lib/slick and community/base/slick/util
  lazy val slick_version = "3.5.2"
  lazy val sttp_version = "3.8.16"
  lazy val swagger_parser_version = "2.1.22"
  lazy val tapir_client_version = "1.9.11"
  lazy val tapir_version = "1.11.7"
  lazy val testcontainers_version = "1.19.7"
  lazy val tink_version = "1.12.0"
  lazy val toxiproxy_java_version = "2.1.7"
  lazy val upickle_version = "4.0.2"

  lazy val reflections = "org.reflections" % "reflections" % reflections_version
  lazy val pureconfig_core =
    "com.github.pureconfig" %% "pureconfig-core" % pureconfig_version exclude ("com.chuusai", s"shapeless_$scala_version_short")
  lazy val pureconfig_cats = "com.github.pureconfig" %% "pureconfig-cats" % pureconfig_version
  lazy val pureconfig_generic =
    "com.github.pureconfig" %% "pureconfig-generic" % pureconfig_version exclude ("com.chuusai", s"shapeless_$scala_version_short")
  lazy val pureconfig_macros = "com.github.pureconfig" %% "pureconfig-macros" % pureconfig_version

  lazy val scala_collection_contrib =
    "org.scala-lang.modules" %% "scala-collection-contrib" % scala_collections_contrib_version
  lazy val scala_reflect = "org.scala-lang" % "scala-reflect" % scala_version
  lazy val shapeless = "com.chuusai" %% "shapeless" % shapeless_version

  lazy val monocle_macro = "dev.optics" %% "monocle-macro" % monocle_version

  // ammonite requires the full scala version including patch number
  lazy val ammonite = "com.lihaoyi" % "ammonite" % ammonite_version cross CrossVersion.full
  lazy val pprint = "com.lihaoyi" %% "pprint" % pprint_version

  lazy val h2 = "com.h2database" % "h2" % h2_version
  lazy val postgres = "org.postgresql" % "postgresql" % postgres_version
  lazy val flyway = "org.flywaydb" % "flyway-core" % flyway_version
  lazy val flyway_postgres = "org.flywaydb" % "flyway-database-postgresql" % flyway_version

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

  lazy val gson = "com.google.code.gson" % "gson" % gson_version
  lazy val gson_fire = "io.gsonfire" % "gson-fire" % gson_fire_version

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

  lazy val pekko_actor = "org.apache.pekko" %% "pekko-actor" % pekko_version
  lazy val pekko_actor_typed = "org.apache.pekko" %% "pekko-actor-typed" % pekko_version
  lazy val pekko_actor_testkit_typed =
    "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekko_version
  lazy val pekko_stream = "org.apache.pekko" %% "pekko-stream" % pekko_version
  lazy val pekko_stream_testkit = "org.apache.pekko" %% "pekko-stream-testkit" % pekko_version
  lazy val pekko_slf4j =
    "org.apache.pekko" %% "pekko-slf4j" % pekko_version excludeAll (incompatibleLogging: _*)
  lazy val pekko_http = "org.apache.pekko" %% "pekko-http" % pekko_http_version
  lazy val pekko_http_testkit = "org.apache.pekko" %% "pekko-http-testkit" % pekko_http_version

  lazy val scala_logging =
    "com.typesafe.scala-logging" %% "scala-logging" % scala_logging_version excludeAll (incompatibleLogging: _*)
  lazy val scalacheck = "org.scalacheck" %% "scalacheck" % scalacheck_version
  lazy val scalatest = "org.scalatest" %% "scalatest" % scalatest_version
  lazy val scalaz_core = "org.scalaz" %% "scalaz-core" % scalaz_version
  lazy val scalatestScalacheck =
    "org.scalatestplus" %% "scalacheck-1-18" % (scalatest_version + ".0")
  lazy val mockito_scala = "org.mockito" %% "mockito-scala" % mockito_scala_version
  lazy val scalatestMockito = "org.scalatestplus" %% "mockito-3-4" % ("3.2.10.0")

  lazy val jupiter_interface = "net.aichler" % "jupiter-interface" % "0.9.0"
  lazy val junit_interface = "com.github.sbt" % "junit-interface" % "0.13.3"

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
  lazy val chimneyJavaConversion = "io.scalaland" %% "chimney-java-collections" % chimney_version
  lazy val jackson_databind_nullable =
    "org.openapitools" % "jackson-databind-nullable" % jackson_databind_nullable_version
  lazy val jakarta_annotation_api =
    "jakarta.annotation" % "jakarta.annotation-api" % jakarta_annotation_api_version
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
  lazy val circe_yaml = "io.circe" %% "circe-yaml" % circe_yaml_version

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

  lazy val opentelemetry_api = resolveDependency("io.opentelemetry", "opentelemetry-api")
  lazy val opentelemetry_sdk = resolveDependency("io.opentelemetry", "opentelemetry-sdk")
  lazy val opentelemetry_sdk_autoconfigure =
    resolveDependency("io.opentelemetry", "opentelemetry-sdk-extension-autoconfigure")
  lazy val opentelemetry_exporter_zipkin =
    resolveDependency("io.opentelemetry", "opentelemetry-exporter-zipkin")
  lazy val opentelemetry_exporter_otlp =
    resolveDependency("io.opentelemetry", "opentelemetry-exporter-otlp")
  lazy val opentelemetry_exporter_common =
    resolveDependency("io.opentelemetry", "opentelemetry-exporter-common")
  lazy val opentelemetry_exporter_prometheus =
    resolveDependency("io.opentelemetry", "opentelemetry-exporter-prometheus")
  lazy val opentelemetry_proto =
    resolveDependency("io.opentelemetry", "opentelemetry-proto")

  lazy val opentelemetry_instrumentation_grpc =
    resolveDependency("io.opentelemetry.instrumentation", "opentelemetry-grpc-1.6")
  lazy val opentelemetry_instrumentation_runtime_metrics =
    resolveDependency("io.opentelemetry.instrumentation", "opentelemetry-runtime-telemetry-java8")
  // https://github.com/open-telemetry/opentelemetry-java-instrumentation/tree/main/instrumentation/hikaricp-3.0/library
  lazy val opentelemetry_instrumentation_hikari =
    "io.opentelemetry.instrumentation" % "opentelemetry-hikaricp-3.0" % opentelemetry_instrumentation_runtime_metrics.revision

  lazy val better_files = "com.github.pathikrit" %% "better-files" % better_files_version

  // TODO(#10852) one database library, not two
  lazy val slick =
    "com.typesafe.slick" %% "slick" % slick_version excludeAll (incompatibleLogging: _*)
  lazy val slick_hikaricp = "com.typesafe.slick" %% "slick-hikaricp" % slick_version

  lazy val testcontainers = "org.testcontainers" % "testcontainers" % testcontainers_version
  lazy val testcontainers_postgresql = "org.testcontainers" % "postgresql" % testcontainers_version

  lazy val sttp = "com.softwaremill.sttp.client3" %% "core" % sttp_version

  // demo dependencies (you also need to update demo.sc)
  lazy val scalafx = "org.scalafx" %% "scalafx" % scalafx_version
  lazy val javafx_all = Seq("controls", "base", "fxml", "media", "web", "graphics").map { x =>
    "org.openjfx" % s"javafx-$x" % scalafx_all_version
  }

  lazy val toxiproxy_java = "eu.rekawek.toxiproxy" % "toxiproxy-java" % toxiproxy_java_version

  lazy val wartremover_dep =
    "org.wartremover" % "wartremover" % wartremover.Wart.PluginVersion cross CrossVersion.full

  lazy val scala_csv = "com.github.tototoshi" %% "scala-csv" % scala_csv_version

  // AWS SDK for Java API to encrypt/decrypt keys using AWS KMS
  lazy val aws_kms = "software.amazon.awssdk" % "kms" % aws_version
  lazy val aws_sts = "software.amazon.awssdk" % "sts" % aws_version

  // GCP SDK for Java API to encrypt/decrypt keys using GCP KMS
  lazy val gcp_kms = "com.google.cloud" % "google-cloud-kms" % gcp_kms_version

  lazy val awaitility = "org.awaitility" % "awaitility" % awaitility_version

  lazy val munit = "org.scalameta" % "munit_2.13" % munit_version

  lazy val protoc_gen_doc = "io.github.pseudomuto" % "protoc-gen-doc" % protoc_gen_doc_version

  // We prevent library conflicts that new tapir creates / some tests fail
  val tapirExclusions = Seq(
    ExclusionRule(
      organization = "io.circe",
      name = "circe-parser_2.13",
    ),
    ExclusionRule(
      organization = "io.circe",
      name = "circe-generic_2.13",
    ),
    ExclusionRule(
      organization = "io.circe",
      name = "circe-core_2.13",
    ),
  )

  lazy val tapir_json_circe =
    "com.softwaremill.sttp.tapir" %% "tapir-json-circe" % tapir_version excludeAll (tapirExclusions: _*)

  lazy val tapir_pekko_http_server =
    "com.softwaremill.sttp.tapir" %% "tapir-pekko-http-server" % tapir_version excludeAll (tapirExclusions: _*)

  lazy val tapir_sttp_client =
    "com.softwaremill.sttp.tapir" %% "tapir-sttp-client" % tapir_client_version excludeAll (tapirExclusions: _*)

  lazy val tapir_openapi_docs =
    "com.softwaremill.sttp.tapir" %% "tapir-openapi-docs" % tapir_version excludeAll (tapirExclusions: _*)

  lazy val tapir_asyncapi_docs =
    "com.softwaremill.sttp.tapir" %% "tapir-asyncapi-docs" % tapir_version excludeAll (tapirExclusions: _*)

  lazy val sttp_apiscpec_openapi_circe_yaml =
    "com.softwaremill.sttp.apispec" %% "openapi-circe-yaml" % apispec_version excludeAll (tapirExclusions: _*)

  lazy val sttp_apiscpec_asyncapi_circe_yaml =
    "com.softwaremill.sttp.apispec" %% "asyncapi-circe-yaml" % apispec_version excludeAll (tapirExclusions: _*)

  lazy val sttp_pekko_backend =
    "com.softwaremill.sttp.client3" %% "pekko-http-backend" % pekko_http_backend_version

  lazy val swagger_parser =
    "io.swagger.parser.v3" % "swagger-parser" % swagger_parser_version

  // Transcode dependencies
  lazy val upickle = "com.lihaoyi" %% "upickle" % upickle_version

  // We have to exclude conflicting parser version
  lazy val ujson_circe =
    "com.lihaoyi" %% "ujson-circe" % upickle_version exclude ("io.circe", "circe-parser_2.13")

  // force override to address issue with transitive unicode lib dependency
  lazy val icu4j_version = "com.ibm.icu" % "icu4j" % "63.1"
  lazy val protostuff_parser = "io.protostuff" % "protostuff-parser" % protostuff_version

  object resolveDependency {
    import io.circe.*, io.circe.parser.*, io.circe.generic.auto.*, io.circe.syntax.*
    import better.files.*

    lazy val ThisProject = "Canton"
    lazy val OtherProjects: Set[String] = Set("DamlSDK")

    case class Dependencies(dependencies: List[Dependency])
    case class Dependency(org: String, artifacts: Seq[String], version: String, users: Set[String])

    private val dependenciesForThisProject =
      decode[Dependencies](file"dependencies.json".contentAsString)
        .valueOr { err =>
          throw new RuntimeException(s"Failed to parse dependencies file: $err")
        }
        .dependencies
        .flatMap {
          case Dependency(org, artifacts, version, users) if users contains ThisProject =>
            artifacts.map(artifact => (org, artifact) -> (org % artifact % version))
          case dep if dep.users.subsetOf(OtherProjects) => Seq.empty
          case invalid =>
            throw new RuntimeException(s"Invalid dependency definition: $invalid")
        }
        .toMap

    def apply(organization: String, artifact: String): ModuleID =
      dependenciesForThisProject
        .get(organization -> artifact)
        .orElse(dependenciesForThisProject.get(organization -> s"${artifact}_2.13"))
        .getOrElse(
          throw new RuntimeException(s"Unknown dependency: $organization, $artifact")
        )
  }

  lazy val daml_script_runner = "com.daml" %% "daml-script-runner" % daml_libraries_version
  lazy val daml_lf_data = "com.daml" %% "daml-lf-data" % daml_libraries_version
  lazy val daml_lf_language = "com.daml" %% "daml-lf-language" % daml_libraries_version
  lazy val daml_libs_scala_contextualized_logging =
    "com.daml" %% "contextualized-logging" % daml_libraries_version
  lazy val daml_test_evidence_tag =
    "com.daml" %% "test-evidence-tag" % daml_libraries_version
  lazy val daml_test_evidence_generator_scalatest =
    "com.daml" %% "test-evidence-generator" % daml_libraries_version
  lazy val daml_lf_archive_reader = "com.daml" %% "daml-lf-archive-reader" % daml_libraries_version
  lazy val daml_lf_engine = "com.daml" %% "daml-lf-engine" % daml_libraries_version
  lazy val daml_lf_transaction = "com.daml" %% "daml-lf-transaction" % daml_libraries_version
  lazy val daml_non_empty = "com.daml" %% "nonempty" % daml_libraries_version
  lazy val daml_nonempty_cats = "com.daml" %% "nonempty-cats" % daml_libraries_version
  lazy val daml_metrics = "com.daml" %% "metrics" % daml_libraries_version
  lazy val daml_tracing = "com.daml" %% "tracing" % daml_libraries_version

  lazy val daml_observability_pekko_http_metrics =
    "com.daml" %% "pekko-http-metrics" % daml_libraries_version
  lazy val daml_executors = "com.daml" %% "executors" % daml_libraries_version
  lazy val daml_timer_utils = "com.daml" %% "timer-utils" % daml_libraries_version
  lazy val daml_libs_struct_spray_json = "com.daml" %% "struct-spray-json" % daml_libraries_version
  lazy val daml_libs_scala_scalatest_utils =
    "com.daml" %% "scalatest-utils" % daml_libraries_version
  lazy val daml_libs_scala_ledger_resources =
    "com.daml" %% "ledger-resources" % daml_libraries_version
  lazy val daml_rs_grpc_pekko = "com.daml" %% "rs-grpc-pekko" % daml_libraries_version
  lazy val daml_lf_encoder = "com.daml" %% "daml-lf-encoder" % daml_libraries_version
  lazy val daml_lf_api_type_signature =
    "com.daml" %% "daml-lf-api-type-signature" % daml_libraries_version
  lazy val daml_rs_grpc_bridge = "com.daml" % "rs-grpc-bridge" % daml_libraries_version
  // Daml testing libs
  lazy val daml_libs_scala_ports = "com.daml" %% "ports" % daml_libraries_version
  lazy val daml_http_test_utils = "com.daml" %% "http-test-utils" % daml_libraries_version
  lazy val daml_testing_utils = "com.daml" %% "testing-utils" % daml_libraries_version
  lazy val daml_metrics_test_lib = "com.daml" %% "metrics-test-lib" % daml_libraries_version
  lazy val daml_lf_transaction_test_lib =
    "com.daml" %% "daml-lf-transaction-test-lib" % daml_libraries_version
  lazy val daml_rs_grpc_testing_utils =
    "com.daml" %% "rs-grpc-testing-utils" % daml_libraries_version
  lazy val daml_observability_tracing_test_lib =
    "com.daml" %% "tracing-test-lib" % daml_libraries_version
  lazy val daml_libs_scala_grpc_test_utils =
    "com.daml" %% "grpc-test-utils" % daml_libraries_version
  lazy val daml_lf_parser =
    "com.daml" %% "daml-lf-parser" % daml_libraries_version
  lazy val daml_lf_archive_encoder =
    "com.daml" %% "daml-lf-archive-encoder" % daml_libraries_version
  lazy val daml_ledger_api_value =
    "com.daml" % "ledger-api-value-proto" % daml_libraries_version
  lazy val daml_ledger_api_value_java =
    "com.daml" % "ledger-api-value-java-proto" % daml_libraries_version
  lazy val daml_ledger_api_value_scala =
    "com.daml" %% "ledger-api-value-scalapb" % daml_libraries_version

  // daml repo dependencies
  lazy val fasterjackson_core = resolveDependency("com.fasterxml.jackson.core", "jackson-core")
  lazy val google_protobuf_java = resolveDependency("com.google.protobuf", "protobuf-java")
  lazy val protobuf_version = google_protobuf_java.revision
  // To override 3.19.2 from the daml repo's maven_install_2.13.json
  lazy val google_protobuf_java_util =
    "com.google.protobuf" % "protobuf-java-util" % protobuf_version

  // Version depends actually on scalapb
  lazy val google_common_protos =
    resolveDependency("com.google.api.grpc", "proto-google-common-protos")

  lazy val commons_io = resolveDependency("commons-io", "commons-io")
  lazy val auth0_java = resolveDependency("com.auth0", "java-jwt")
  lazy val auth0_jwks = resolveDependency("com.auth0", "jwks-rsa")
  lazy val guava = resolveDependency("com.google.guava", "guava")
  // TODO(#10852) one database library, not two
  lazy val anorm = resolveDependency("org.playframework.anorm", "anorm")
  lazy val scalapb_json4s = resolveDependency("com.thesamet.scalapb", "scalapb-json4s")
}
