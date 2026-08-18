package com.digitalasset.canton

import com.daml.metrics.ExecutorServiceMetrics
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.config.{CacheConfig, CryptoConfig, SessionEncryptionKeyCacheConfig}
import com.digitalasset.canton.crypto.CryptoTestHelper.TestMessage
import com.digitalasset.canton.crypto.EncryptionAlgorithmSpec.{EciesHkdfHmacSha256Aes128Cbc, RsaOaepSha256}
import com.digitalasset.canton.crypto.EncryptionKeySpec.{EcP256, Rsa2048}
import com.digitalasset.canton.crypto.provider.jce.JceCrypto
import com.digitalasset.canton.crypto.store.memory.{InMemoryCryptoPrivateStore, InMemoryCryptoPublicStore}
import com.digitalasset.canton.crypto.{Crypto, CryptoSchemes, CryptoTestHelper, EncryptionPublicKey}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.google.protobuf.ByteString
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration

@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(3)
@Threads(1)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
class AsymmetricEncryptionBenchmark extends TestEssentials {

  implicit val ec: scala.concurrent.ExecutionContext = Threading.newExecutionContext(
    loggerFactory.threadName + "-env-ec",
    noTracingLogger,
    new ExecutorServiceMetrics(NoOpMetricsFactory),
  )

  private val dataToEncrypt: TestMessage = TestMessage(ByteString.copyFromUtf8("foobar"))

  private val crypto: Crypto = {
    val config = CryptoConfig()
    JceCrypto
      .create(
        config,
        CryptoSchemes.tryFromConfig(config),
        SessionEncryptionKeyCacheConfig(),
        CacheConfig(PositiveNumeric.tryCreate(1)),
        new InMemoryCryptoPrivateStore(testedReleaseProtocolVersion, loggerFactory),
        new InMemoryCryptoPublicStore(loggerFactory),
        CommonMockMetrics.cryptoMetrics,
        timeouts,
        loggerFactory,
      ).fold(err => throw new RuntimeException(s"Failed to create a JCE crypto provider: $err"), identity)
  }

  private val pubKeyEcP256: EncryptionPublicKey =
    Await.result(crypto.generateEncryptionKey(EcP256).value
        .failOnShutdownToAbortException("prepareRun"), Duration.Inf)
        .fold(err => throw new RuntimeException(s"Failed to create a EcP256 key: $err"), identity)

  private val pubKeyRsa2048: EncryptionPublicKey =  Await.result(crypto.generateEncryptionKey(Rsa2048).value
      .failOnShutdownToAbortException("prepareRun"), Duration.Inf)
    .fold(err => throw new RuntimeException(s"Failed to create a Rsa2048 key: $err"), identity)

  @Benchmark
  def encryptEcP256(blackhole: Blackhole): Unit = {
    val res = crypto.pureCrypto.encryptWith(dataToEncrypt, pubKeyEcP256, EciesHkdfHmacSha256Aes128Cbc)
      .fold(err => throw new RuntimeException(s"Failed to encrypt with a EcP256 key: $err"), identity)
    blackhole.consume(res)
  }

  @Benchmark
  def encryptRsa2048(blackhole: Blackhole): Unit = {
    val res = crypto.pureCrypto.encryptWith(dataToEncrypt, pubKeyRsa2048, RsaOaepSha256)
      .fold(err => throw new RuntimeException(s"Failed to encrypt with a Rsa2048 key: $err"), identity)
    blackhole.consume(res)
  }
}
