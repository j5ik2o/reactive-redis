# reactive-redis

[![Build Status](https://travis-ci.org/j5ik2o/reactive-redis.svg?branch=master)](https://travis-ci.org/j5ik2o/reactive-redis)

This is an Akka-Stream based Redis Client for Scala

## Concept

- Depends akka-stream 2.4.x
- Back-pressure support

## Installation

Add the following to your sbt build (Scala 2.11.x, 2.12.x):

### Release Version

```scala
resolvers += "Sonatype OSS Release Repository" at "https://oss.sonatype.org/content/repositories/releases/"

libraryDependencies += "com.github.j5ik2o" %% "reactive-redis" % "1.0.0"
```

### Snapshot Version

```scala
resolvers += "Sonatype OSS Snapshot Repository" at "https://oss.sonatype.org/content/repositories/snapshots/"

libraryDependencies += "com.github.j5ik2o" %% "reactive-redis" % "1.0.1-SNAPSHOT"
```

## Usage

- Future API

```scala
val redisClient = RedisClient(host = "127.0.0.1", timeout = 10 seconds)
val result: Future[String] = for {
  _      <- redisClient.set(key, value)
  result <- redisClient.get(key)
} yield result
```

- Actor API

```scala
val actorRef = system.actorOf(RedisActor.props(UUID.randomUUID, "127.0.0.1", testServer.address.get.getPort))
actorRef ! GetRequest(UUID.randomUUID, id)
```


## License

MIT License / Copyright (c) 2016 Junichi Kato
