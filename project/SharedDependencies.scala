// Source of truth for dependency versions shared with the SDK repo.
// `generateSharedDependencies` renders shared_dependencies.json from this file;
// `checkSharedDependenciesUpToDate` (wired into `lint`) keeps it honest.
import io.circe.KeyEncoder
import io.circe.syntax.EncoderOps
import sbt.{io as _, *}

import java.lang.reflect.Modifier
import scala.collection.immutable.SortedMap

object SharedDependencies {

  // Version constants shared by a family of artifacts that are always bumped together.
  private val opentelemetry_version = "1.60.1"
  private val opentelemetry_alpha_version = "1.60.1-alpha"
  private val otel_instrumentation_version = "2.26.1"
  private val otel_instrumentation_alpha_version = "2.26.1-alpha"
  private val grpc_version = "1.81.0"
  private val pekko_actor_version = "1.2.1"
  private val pekko_http_version = "1.1.0"
  private val bouncycastle_version = "1.86"
  private val jline_version = "3.27.1"
  // Mirrors the scalapb the sbt-protoc plugin generates with (project/plugins.sbt), asserted
  // below so the json cannot pin a version the build never resolves.
  private val scalapb_version = "0.11.14"
  private val scalapb_protoc_version = "0.9.5"
  private val logback_version = "1.6.3"
  private val jackson_version = "2.22.2"
  private val protobuf_version = "4.35.1"
  private val reactivestreams_version = "1.0.4"
  private val scalameter_version = "0.21"
  private val log4j_version = "2.25.4"
  private val anorm_version = "2.7.0"

  require(
    scalapb_version == scalapb.compiler.Version.scalapbVersion,
    s"scalapb_version $scalapb_version does not match the sbt-protoc plugin's "
      + scalapb.compiler.Version.scalapbVersion,
  )

  lazy val logback_classic = "ch.qos.logback" % "logback-classic" % logback_version
  lazy val logback_core = "ch.qos.logback" % "logback-core" % logback_version
  lazy val auth0_java = "com.auth0" % "java-jwt" % "4.2.1"
  lazy val auth0_jwks = "com.auth0" % "jwks-rsa" % "0.21.2"
  lazy val jackson_annotations = "com.fasterxml.jackson.core" % "jackson-annotations" % "2.22"
  lazy val fasterjackson_core = "com.fasterxml.jackson.core" % "jackson-core" % jackson_version
  lazy val jackson_databind = "com.fasterxml.jackson.core" % "jackson-databind" % jackson_version
  lazy val google_common_protos = "com.google.api.grpc" % "proto-google-common-protos" % "2.59.2"
  lazy val google_findbugs = "com.google.code.findbugs" % "jsr305" % "3.0.2"
  lazy val guava = "com.google.guava" % "guava" % "33.3.0-jre"
  lazy val google_protobuf_java = "com.google.protobuf" % "protobuf-java" % protobuf_version
  lazy val google_protobuf_java_util =
    "com.google.protobuf" % "protobuf-java-util" % protobuf_version
  lazy val lihaoyi_sourcecode_2 = "com.lihaoyi" % "sourcecode_2.13" % "0.3.0"
  lazy val scalameter_core = "com.storm-enroute" % "scalameter-core_2.13" % scalameter_version
  lazy val scalameter = "com.storm-enroute" % "scalameter_2.13" % scalameter_version
  lazy val lenses = "com.thesamet.scalapb" % "lenses_2.13" % scalapb_version
  lazy val scalapb_json4s = "com.thesamet.scalapb" % "scalapb-json4s_2.13" % "0.11.1"
  lazy val scalapb_runtime_grpc =
    "com.thesamet.scalapb" % "scalapb-runtime-grpc_2.13" % scalapb_version
  lazy val scalapb_runtime =
    "com.thesamet.scalapb" % "scalapb-runtime_2.13" % scalapb_version
  lazy val typesafe_config = "com.typesafe" % "config" % "1.4.5"
  lazy val commons_io = "commons-io" % "commons-io" % "2.11.0"
  lazy val io_grpc_grpc_api = "io.grpc" % "grpc-api" % grpc_version
  lazy val grpc_context = "io.grpc" % "grpc-context" % grpc_version
  lazy val io_grpc_grpc_core = "io.grpc" % "grpc-core" % grpc_version
  lazy val io_grpc_grpc_inprocess = "io.grpc" % "grpc-inprocess" % grpc_version
  lazy val io_grpc_grpc_netty_shaded = "io.grpc" % "grpc-netty-shaded" % grpc_version
  lazy val io_grpc_grpc_protobuf = "io.grpc" % "grpc-protobuf" % grpc_version
  lazy val grpc_protobuf_lite = "io.grpc" % "grpc-protobuf-lite" % grpc_version
  lazy val io_grpc_grpc_services = "io.grpc" % "grpc-services" % grpc_version
  lazy val io_grpc_grpc_stub = "io.grpc" % "grpc-stub" % grpc_version
  lazy val grpc_util = "io.grpc" % "grpc-util" % grpc_version
  lazy val opentelemetry_instrumentation_grpc =
    "io.opentelemetry.instrumentation" % "opentelemetry-grpc-1.6" % otel_instrumentation_alpha_version
  // https://github.com/open-telemetry/opentelemetry-java-instrumentation/tree/main/instrumentation/hikaricp-3.0/library
  lazy val opentelemetry_instrumentation_hikari =
    "io.opentelemetry.instrumentation" % "opentelemetry-hikaricp-3.0" % otel_instrumentation_alpha_version
  lazy val opentelemetry_instrumentation_api =
    "io.opentelemetry.instrumentation" % "opentelemetry-instrumentation-api" % otel_instrumentation_version
  lazy val opentelemetry_instrumentation_api_incubator =
    "io.opentelemetry.instrumentation" % "opentelemetry-instrumentation-api-incubator" % otel_instrumentation_alpha_version
  lazy val opentelemetry_instrumentation_runtime_metrics =
    "io.opentelemetry.instrumentation" % "opentelemetry-runtime-telemetry-java8" % otel_instrumentation_alpha_version
  lazy val opentelemetry_semconv = "io.opentelemetry.semconv" % "opentelemetry-semconv" % "1.40.0"
  lazy val opentelemetry_api = "io.opentelemetry" % "opentelemetry-api" % opentelemetry_version
  lazy val opentelemetry_api_incubator =
    "io.opentelemetry" % "opentelemetry-api-incubator" % opentelemetry_alpha_version
  lazy val opentelemetry_context =
    "io.opentelemetry" % "opentelemetry-context" % opentelemetry_version
  lazy val opentelemetry_exporter_common =
    "io.opentelemetry" % "opentelemetry-exporter-common" % opentelemetry_version
  lazy val opentelemetry_exporter_otlp =
    "io.opentelemetry" % "opentelemetry-exporter-otlp" % opentelemetry_version
  lazy val opentelemetry_exporter_otlp_common =
    "io.opentelemetry" % "opentelemetry-exporter-otlp-common" % opentelemetry_version
  lazy val opentelemetry_exporter_prometheus =
    "io.opentelemetry" % "opentelemetry-exporter-prometheus" % opentelemetry_alpha_version
  lazy val opentelemetry_exporter_sender_okhttp =
    "io.opentelemetry" % "opentelemetry-exporter-sender-okhttp" % opentelemetry_version
  lazy val opentelemetry_exporter_zipkin =
    "io.opentelemetry" % "opentelemetry-exporter-zipkin" % opentelemetry_version
  lazy val opentelemetry_sdk = "io.opentelemetry" % "opentelemetry-sdk" % opentelemetry_version
  lazy val opentelemetry_sdk_common =
    "io.opentelemetry" % "opentelemetry-sdk-common" % opentelemetry_version
  lazy val opentelemetry_sdk_extension_autoconfigure_spi =
    "io.opentelemetry" % "opentelemetry-sdk-extension-autoconfigure-spi" % opentelemetry_version
  lazy val opentelemetry_sdk_logs =
    "io.opentelemetry" % "opentelemetry-sdk-logs" % opentelemetry_version
  lazy val opentelemetry_sdk_metrics =
    "io.opentelemetry" % "opentelemetry-sdk-metrics" % opentelemetry_version
  lazy val opentelemetry_sdk_testing =
    "io.opentelemetry" % "opentelemetry-sdk-testing" % opentelemetry_version
  lazy val opentelemetry_sdk_trace =
    "io.opentelemetry" % "opentelemetry-sdk-trace" % opentelemetry_version
  lazy val spray_json = "io.spray" % "spray-json_2.13" % "1.3.6"
  lazy val logstash = "net.logstash.logback" % "logstash-logback-encoder" % "6.6"
  lazy val commons_compress = "org.apache.commons" % "commons-compress" % "1.24.0"
  lazy val commons_lang3 = "org.apache.commons" % "commons-lang3" % "3.18.0"
  lazy val log4j_api = "org.apache.logging.log4j" % "log4j-api" % log4j_version
  lazy val log4j_core = "org.apache.logging.log4j" % "log4j-core" % log4j_version
  lazy val pekko_actor_testkit_typed =
    "org.apache.pekko" % "pekko-actor-testkit-typed_2.13" % pekko_actor_version
  lazy val pekko_actor_typed =
    "org.apache.pekko" % "pekko-actor-typed_2.13" % pekko_actor_version
  lazy val pekko_actor =
    "org.apache.pekko" % "pekko-actor_2.13" % pekko_actor_version
  lazy val pekko_http_core = "org.apache.pekko" % "pekko-http-core_2.13" % pekko_http_version
  lazy val pekko_http_testkit =
    "org.apache.pekko" % "pekko-http-testkit_2.13" % pekko_http_version
  lazy val pekko_http = "org.apache.pekko" % "pekko-http_2.13" % pekko_http_version
  lazy val pekko_parsing = "org.apache.pekko" % "pekko-parsing_2.13" % pekko_http_version
  lazy val pekko_protobuf_v3 = "org.apache.pekko" % "pekko-protobuf-v3_2.13" % pekko_actor_version
  // Excludes slf4j 1.x and logback 1.2.x transitive dependencies, incompatible with the slf4j 2.x
  // and logback 1.4.x used here.
  lazy val pekko_slf4j =
    "org.apache.pekko" % "pekko-slf4j_2.13" % pekko_actor_version excludeAll
      (ExclusionRule("org.slf4j"), ExclusionRule("ch.qos.logback"))
  lazy val pekko_stream_testkit =
    "org.apache.pekko" % "pekko-stream-testkit_2.13" % pekko_actor_version
  lazy val pekko_stream =
    "org.apache.pekko" % "pekko-stream_2.13" % pekko_actor_version
  lazy val pekko_testkit = "org.apache.pekko" % "pekko-testkit_2.13" % pekko_actor_version
  lazy val bouncycastle_bcpkix = "org.bouncycastle" % "bcpkix-jdk18on" % bouncycastle_version
  lazy val bouncycastle_bcprov = "org.bouncycastle" % "bcprov-jdk18on" % bouncycastle_version
  lazy val bcutil_jdk18on = "org.bouncycastle" % "bcutil-jdk18on" % bouncycastle_version
  lazy val jline = "org.jline" % "jline" % jline_version
  lazy val jline_reader = "org.jline" % "jline-reader" % jline_version
  lazy val jline_terminal = "org.jline" % "jline-terminal" % jline_version
  lazy val jline_terminal_jna = "org.jline" % "jline-terminal-jna" % "3.14.1"
  lazy val anorm_tokenizer = "org.playframework.anorm" % "anorm-tokenizer_2.13" % anorm_version
  lazy val anorm = "org.playframework.anorm" % "anorm_2.13" % anorm_version
  lazy val reactivestreams = "org.reactivestreams" % "reactive-streams" % reactivestreams_version
  lazy val reactivestreams_examples =
    "org.reactivestreams" % "reactive-streams-examples" % reactivestreams_version
  lazy val reactivestreams_tck =
    "org.reactivestreams" % "reactive-streams-tck" % reactivestreams_version
  lazy val scalaz_scalacheck_binding =
    "org.scalaz" % "scalaz-scalacheck-binding_2.13" % "7.2.33-scalacheck-1.15"
  lazy val org_slf4j_jul_to_slf4j = "org.slf4j" % "jul-to-slf4j" % "2.0.6"

  // Only daml consumes these, via the generated json. Nothing in this build declares or
  // resolves them, so the versions pin nothing here.
  // TODO(#30144): declare them in daml's own artifact list and drop them from here.
  lazy val compilerplugin = "com.thesamet.scalapb" % "compilerplugin_2.13" % scalapb_version
  lazy val protoc_bridge = "com.thesamet.scalapb" % "protoc-bridge_2.13" % scalapb_protoc_version
  lazy val protoc_gen = "com.thesamet.scalapb" % "protoc-gen_2.13" % scalapb_protoc_version
  lazy val proto_google_common_protos_scalapb_0_11 =
    "com.thesamet.scalapb.common-protos" % "proto-google-common-protos-scalapb_0.11_2.13" % "2.9.6-0"
  lazy val spray_json_derived_codecs =
    "io.github.paoloboni" % "spray-json-derived-codecs_2.13" % "2.3.10"
  lazy val pekko_http_spray_json =
    "org.apache.pekko" % "pekko-http-spray-json_2.13" % pekko_http_version
  lazy val jline_native = "org.jline" % "jline-native" % jline_version
  lazy val sangria_spray_json = "org.sangria-graphql" % "sangria-spray-json_2.13" % "1.0.2"

  // Every public 0-arg method returning a ModuleID is one of the lazy vals above, so there is no
  // separate map to keep in sync. NOTE: select on visibility. Each lazy val also compiles to a
  // non-public `x$lzycompute()` of the same signature, which scalac does not flag synthetic.
  lazy val allModuleIds: Seq[ModuleID] = {
    val moduleIdClass = classOf[ModuleID]
    val (accessors, internal) = getClass.getDeclaredMethods.toSeq
      .filter(m => m.getParameterCount == 0 && moduleIdClass.isAssignableFrom(m.getReturnType))
      .partition(m => Modifier.isPublic(m.getModifiers))
    // The non-public half is the `x$lzycompute()` initializers. A private lazy val lands here
    // too and would never reach the json, so name it rather than drop a pin in silence.
    val dropped = internal.filterNot(_.getName.contains("$")).map(_.getName)
    require(
      dropped.isEmpty,
      s"private ModuleID vals never reach the json: ${dropped.mkString(", ")}",
    )
    // A lazy val that fails to initialize must break the build, not silently drop a pin that
    // the SDK repo reads out of the generated json.
    accessors.map(_.invoke(this).asInstanceOf[ModuleID])
  }

  // One field of the generated json: the artifact and the version pinned for it.
  private final case class SharedDependencyEntry(
      organization: String,
      name: String,
      version: String,
  ) {
    def coordinate: String = s"$organization:$name"
  }

  private object SharedDependencyEntry {
    implicit val keyEncoder: KeyEncoder[SharedDependencyEntry] =
      KeyEncoder.instance(_.coordinate)
    // Keys the json in (organization, name) order, which differs from ordering by the rendered
    // "organization:name" wherever one organization is a prefix of another.
    implicit val ordering: Ordering[SharedDependencyEntry] =
      Ordering.by(entry => (entry.organization, entry.name))
  }

  // A coordinate declared twice would reach the json as a repeated key, and a reader keeping the
  // last one would silently pick a version this build does not resolve. The SortedMap drops one
  // of the two without a word, so reject it before building the map.
  private def sharedDependencies: SortedMap[SharedDependencyEntry, String] = {
    val declared =
      allModuleIds.map(m => SharedDependencyEntry(m.organization, m.name, m.revision))
    val duplicates = declared
      .groupBy(_.coordinate)
      .collect {
        case (coordinate, pinned) if pinned.size > 1 =>
          s"$coordinate (${pinned.map(_.version).sorted.mkString(", ")})"
      }
    require(
      duplicates.isEmpty,
      s"duplicate module ids: ${duplicates.toSeq.sorted.mkString(", ")}",
    )
    SortedMap(declared.map(entry => entry -> entry.version): _*)
  }

  // The SortedMap orders the keys; spaces2 supplies the 2-space indent and the space on both
  // sides of the colon, which is the shape the SDK repo reads.
  lazy val json: String = sharedDependencies.asJson.spaces2 + "\n"

}
