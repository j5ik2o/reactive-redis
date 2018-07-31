package com.github.j5ik2o.reactive.redis.command

import java.util.concurrent.TimeUnit

import cats.implicits._
import com.github.j5ik2o.reactive.redis.BenchmarkHelper
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
@State(Scope.Benchmark)
class Get extends BenchmarkHelper {

  override def fixture(): Unit = {
    Await.result(pool.withConnectionF { con =>
      client.set("A", "value").run(con)
    }.runAsync, Duration.Inf)
  }

  @Benchmark
  def reactiveRedis(): Unit = {
    Await.result(pool.withConnectionF { con =>
      client.get("A").run(con)
    }.runAsync, Duration.Inf)
    ()
  }

  @Benchmark
  def jedis: Unit = {
    val jedis = jedisPool.getResource
    jedis.get("A")
    jedis.close()
  }

  @Benchmark
  def rediscala(): Unit = {
    Await.result(rediscalaPool.get("A"), Duration.Inf)
    ()
  }

  @Setup(Level.Trial)
  override def setup(): Unit = super.setup()

  @TearDown(Level.Trial)
  override def tearDown(): Unit = super.tearDown()
}
