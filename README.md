# reactive-redis

[![CircleCI](https://circleci.com/gh/j5ik2o/reactive-redis/tree/master.svg?style=svg)](https://circleci.com/gh/j5ik2o/reactive-redis/tree/master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/54e35f1cfde048feae3c9c35cadf78a0)](https://www.codacy.com/app/j5ik2o/reactive-redis?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=j5ik2o/reactive-redis&amp;utm_campaign=Badge_Grade)

Akka-Stream based Redis Client for Scala

## Concept

- Transport is akka-stream 2.5.x.
- Response parser is fastparse.
- monix.eval.Task support.

## Installation

Add the following to your sbt build (Scala 2.11.x, 2.12.x):

### Release Version

```scala
resolvers += "Sonatype OSS Release Repository" at "https://oss.sonatype.org/content/repositories/releases/"

libraryDependencies += "com.github.j5ik2o" %% "reactive-redis-core" % "1.0.6"
```

### Snapshot Version

```scala
resolvers += "Sonatype OSS Snapshot Repository" at "https://oss.sonatype.org/content/repositories/snapshots/"

libraryDependencies += "com.github.j5ik2o" %% "reactive-redis-core" % "1.0.7-SNAPSHOT"
```

## Usage

### Non connection pooling

```scala
import monix.execution.Scheduler.Implicits.global

implicit val system = ActorSystem()

val peerConfig = PeerConfig(remoteAddress = new InetSocketAddress("127.0.0.1", 6379))
val connection = RedisConnection(peerConfig)
val client = RedisClient()

val result = (for{
  _ <- client.set("foo", "bar")
  r <- client.get("foo")
} yield r).run(connection).runAsync

println(result) // bar
```

### Connection pooling

```scala
import monix.execution.Scheduler.Implicits.global

implicit val system = ActorSystem()

val peerConfig = PeerConfig(remoteAddress = new InetSocketAddress("127.0.0.1", 6379))
val pool = RedisConnectionPool.ofRoundRobin(sizePerPeer = 5, Seq(peerConfig), RedisConnection(_)) // powered by RoundRobinPool
val connection = RedisConnection(connectionConfig)
val client = RedisClient()

// Fucntion style
val result1 = pool.withConnectionF{ con =>
  (for{
    _ <- client.set("foo", "bar")
    r <- client.get("foo")
  } yield r).run(con) 
}.runAsync

println(result1) // bar

// Monadic style
val result2 = (for {
  _ <- ConnectionAutoClose(pool)(client.set("foo", "bar").run)
  r <- ConnectionAutoClose(pool)(client.get("foo").run)
} yield r).run().runAsync

println(result2) // bar
```

if you want to use other pooling implementation, please select from the following modules.

- reactive-redis-pool-commons (commons-pool2)
- reactive-redis-pool-scala (scala-pool)
- reactive-redis-pool-fop (fast-object-pool)
- reactive-redis-pool-stormpot (stormpot)

### Master & Slaves aggregate connection

```scala
import monix.execution.Scheduler.Implicits.global

implicit val system = ActorSystem()

val masterPeerConfig = PeerConfig(remoteAddress = new InetSocketAddress("127.0.0.1", 6379))
val slavePeerConfigs = Seq(
  PeerConfig(remoteAddress = new InetSocketAddress("127.0.0.1", 6380)),
  PeerConfig(remoteAddress = new InetSocketAddress("127.0.0.1", 6381)),
  PeerConfig(remoteAddress = new InetSocketAddress("127.0.0.1", 6382))
)

val connection = new RedisMasterSlavesConnection(
  masterConnectionPoolFactory = RedisConnectionPool.ofRoundRobin(sizePerPeer = 2, Seq(masterPeerConfig), RedisConnection(_)),
  slaveConnectionPoolFactory = RedisConnectionPool.ofRoundRobin(sizePerPeer = 2, slavePeerConfigs, RedisConnection(_))
)

val client = RedisClient()

val result = (for{
  _ <- client.set("foo", "bar") // write to master
  r <- client.get("foo")        // read from any slave
} yield r).run(connection).runAsync

println(result) // bar
```

## License

MIT License / Copyright (c) 2016 Junichi Kato

