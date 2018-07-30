package com.github.j5ik2o.reactive.redis.command

import java.util.concurrent.TimeUnit

import com.github.j5ik2o.reactive.redis.{ BenchmarkHelper, Result }
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
@State(Scope.Thread)
class Ping extends BenchmarkHelper {
  override def fixture(): Unit = {}

  @Benchmark
  def reactiveRedis: Unit = {
    Await.result(pool.withConnectionF { con =>
      client.ping().run(con)
    }.runAsync, Duration.Inf)
    ()
  }

  @Benchmark
  def rediscala: Unit = {
    Await.result(rediscalaPool.ping(), Duration.Inf)
    ()
  }

  @Setup(Level.Trial)
  override def setup(): Unit = super.setup()

  @TearDown(Level.Trial)
  override def tearDown(): Unit = super.tearDown()

}
