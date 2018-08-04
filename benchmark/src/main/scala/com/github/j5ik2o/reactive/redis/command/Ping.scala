package com.github.j5ik2o.reactive.redis.command

import java.util.concurrent.TimeUnit

import com.github.j5ik2o.reactive.redis.BenchmarkHelper
import monix.eval.Task
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
  def reactiveRedisOfDefaultQueue(): Unit = {
    Await.result(reactiveRedisPoolOfDefaultQueue.withConnectionF { con =>
      client.ping().run(con)
    }.runAsync, Duration.Inf)
    ()
  }

  @Benchmark
  def reactiveRedisOfDefaultActor(): Unit = {
    Await.result(reactiveRedisPoolOfDefaultActor.withConnectionF { con =>
      client.ping().run(con)
    }.runAsync, Duration.Inf)
    ()
  }

  @Benchmark
  def reactiveRedisOfJedisQueue(): Unit = {
    Await.result(reactiveRedisPoolOfJedisQueue.withConnectionF { con =>
      client.ping().run(con)
    }.runAsync, Duration.Inf)
    ()
  }

  @Benchmark
  def reactiveRedisOfJedisActor(): Unit = {
    Await.result(reactiveRedisPoolOfJedisActor.withConnectionF { con =>
      client.ping().run(con)
    }.runAsync, Duration.Inf)
    ()
  }

  @Benchmark
  def jedis: Unit = {
    Await.result(Task {
      val jedis = jedisPool.getResource
      jedis.ping()
      jedis.close()
    }.runAsync, Duration.Inf)
    ()
  }

  @Benchmark
  def rediscala: Unit = {
    Await.result(rediscalaPool.ping(), Duration.Inf)
    ()
  }

  @Benchmark
  def scalaRedis(): Unit = {
    Await.result(Task {
      scalaRedisPool.withClient { client =>
        client.ping
      }
    }.runAsync, Duration.Inf)
    ()
  }

  @Setup(Level.Trial)
  override def setup(): Unit = super.setup()

  @TearDown(Level.Trial)
  override def tearDown(): Unit = super.tearDown()

}
