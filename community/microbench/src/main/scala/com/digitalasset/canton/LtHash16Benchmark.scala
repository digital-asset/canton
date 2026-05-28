package com.digitalasset.canton

import com.digitalasset.canton.crypto.{LtHash16, LtHash16Blake3}
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.util.Random
import java.util.concurrent.TimeUnit

@State(Scope.Thread)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Fork(3)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
class LtHash16Benchmark {
  private val input: Array[Byte] = new Array[Byte](80)
  private val rnd = new Random(42L)

  def setup(): Unit =
    rnd.nextBytes(input)

  @Benchmark
  def addOneBlake2(blackhole: Blackhole): Unit = {
    val h = LtHash16()
    h.add(input)
    blackhole.consume(h.getByteString())
  }

  @Benchmark
  def addOneBlake3(blackhole: Blackhole): Unit = {
    val h = LtHash16Blake3.empty
    h.add(input)
    blackhole.consume(h.getByteString)
  }
}
