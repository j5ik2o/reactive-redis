# reactive-redis

[![CircleCI](https://circleci.com/gh/j5ik2o/reactive-redis.svg?style=shield&circle-token=b858c698c54b46769e933d7ee7fd55209234bae1)](https://circleci.com/gh/j5ik2o/reactive-redis)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.j5ik2o/reactive-redis-core_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.j5ik2o/reactive-redis-core_2.12)
[![Scaladoc](http://javadoc-badge.appspot.com/com.github.j5ik2o/reactive-redis-core_2.12.svg?label=scaladoc)](http://javadoc-badge.appspot.com/com.github.j5ik2o/reactive-redis-core_2.12/com/github/j5ik2o/reactive/redis/index.html?javadocio=true)
[![License: MIT](http://img.shields.io/badge/license-MIT-orange.svg)](LICENSE)

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

libraryDependencies += "com.github.j5ik2o" %% "reactive-redis-core" % "1.0.15"
```

### Snapshot Version

```scala
resolvers += "Sonatype OSS Snapshot Repository" at "https://oss.sonatype.org/content/repositories/snapshots/"

libraryDependencies += "com.github.j5ik2o" %% "reactive-redis-core" % "1.0.16-SNAPSHOT"
```

## Support Commands

- Cluster

<table>
  <tr>
    <td>Command</td>
    <td>Support</td>
  </tr>
  <tr>
    <td>CLUSTER ADDSLOTS</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>CLUSTER ADDSLOTS</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>CLUSTER ADDSCLUSTER COUNT-FAILURE-REPORTSLOTS</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>CLUSTER COUNTKEYSINSLOT</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>CLUSTER DELSLOTS</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>CLUSTER FAILOVER</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>CLUSTER FORGET</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>CLUSTER GETKEYSINSLOT</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>CLUSTER INFO</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>CLUSTER KEYSLOT</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>CLUSTER MEET</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>CLUSTER NODES</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>CLUSTER REPLICATE</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>CLUSTER RESET</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>CLUSTER SAVECONFIG</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>CLUSTER SET-CONFIG-EPOCH</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>CLUSTER SETSLOT</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>CLUSTER SLAVES</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>CLUSTER SLOTS</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>READONLY</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>READWRITE</td>
    <td>TODO</td>
  </tr>
</table>

- Connection

<table>
  <tr>
    <td>Command</td>
    <td>Support</td>
  </tr>
  <tr>
    <td>AUTH</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>ECHO</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>PING</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>QUIT</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>SELECT</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>SWAPDB</td>
    <td>Supported</td>
  </tr>
</table>

- Geo

<table>
  <tr>
    <td>Command</td>
    <td>Support</td>
  </tr>
  <tr>
    <td>GEOADD</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>GEODIST</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>GEOHASH</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>GEOPOS</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>GEORADIUS</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>GEORADIUSBYMEMBER</td>
    <td>TODO</td>
  </tr>
</table>

- Hashes

<table>
  <tr>
    <td>Command</td>
    <td>Support</td>
  </tr>
  <tr>
    <td>HDEL</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>HEXISTS</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>HGET</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>HGETALL</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>HINCRBY</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>HINCRBYFLOAT</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>HKEYS</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>HLEN</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>HMGET</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>HMSET</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>HSCAN</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>HSET</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>HSETNX</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>HSTRLEN</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>HVALS</td>
    <td>TODO</td>
  </tr>
</table>

- HyperLogLog

<table>
  <tr>
    <td>Command</td>
    <td>Support</td>
  </tr>
  <tr>
    <td>PFADD</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>PFCOUNT</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>PFMERGE</td>
    <td>TODO</td>
  </tr>
</table>

- Keys

<table>
  <tr>
    <td>Command</td>
    <td>Support</td>
  </tr>
  <tr>
    <td>DEL</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>DUMP</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>EXISTS</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>EXPIRE</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>EXPIREAT</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>KEYS</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>MIGRATE</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>MOVE</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>OBJECT</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>PERSIST</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>PEXPIRE</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>PEXPIREAT</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>PTTL</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>RANDOMKEY</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>RENAME</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>RENAMENX</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>RESTORE</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>SCAN</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>SORT</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>TOUCH</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>TTL</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>TYPE</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>UNLINK</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>WAIT</td>
    <td>Supported</td>
  </tr>
</table>

- Lists

<table>
  <tr>
    <td>BLPOP</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>BRPOP</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>BRPOPLPUSH</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>LINDEX</td>
    <td></td>
  </tr>
  <tr>
    <td>LINSERT</td>
    <td></td>
  </tr>
  <tr>
    <td>LLEN</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>LPOP</td>
    <td></td>
  </tr>
  <tr>
    <td>LPUSH</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>LPUSHX</td>
    <td></td>
  </tr>
  <tr>
    <td>LRANGE</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>LREM</td>
    <td></td>
  </tr>
  <tr>
    <td>LSET</td>
    <td></td>
  </tr>
  <tr>
    <td>LTRIM</td>
    <td></td>
  </tr>
  <tr>
    <td>RPOP</td>
    <td></td>
  </tr>
  <tr>
    <td>RPOPLPUSH</td>
    <td></td>
  </tr>
  <tr>
    <td>RPUSH</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>RPUSHX</td>
    <td></td>
  </tr>
</table>

- Pub/Sub

<table>
  <tr>
    <td>PSUBSCRIBE</td>
    <td></td>
  </tr>
  <tr>
    <td>PUBLISH</td>
    <td></td>
  </tr>
  <tr>
    <td>PUBSUB</td>
    <td></td>
  </tr>
  <tr>
    <td>PUNSUBSCRIBE</td>
    <td></td>
  </tr>
  <tr>
    <td>SUBSCRIBE</td>
    <td></td>
  </tr>
  <tr>
    <td>UNSUBSCRIBE</td>
    <td></td>
  </tr>
</table>

- Scripting

<table>
  <tr>
    <td>EVAL</td>
    <td></td>
  </tr>
  <tr>
    <td>EVALSHA</td>
    <td></td>
  </tr>
  <tr>
    <td>SCRIPT DEBUG</td>
    <td></td>
  </tr>
  <tr>
    <td>SCRIPT EXISTS</td>
    <td></td>
  </tr>
  <tr>
    <td>SCRIPT FLUSH</td>
    <td></td>
  </tr>
  <tr>
    <td>SCRIPT KILL</td>
    <td></td>
  </tr>
  <tr>
    <td>SCRIPT LOAD</td>
    <td></td>
  </tr>
</table>

- Server

<table>
  <tr>
    <td>BGREWRITEAOF</td>
    <td></td>
  </tr>
  <tr>
    <td>BGSAVE</td>
    <td></td>
  </tr>
  <tr>
    <td>CLIENT GETNAME</td>
    <td></td>
  </tr>
  <tr>
    <td>CLIENT KILL</td>
    <td></td>
  </tr>
  <tr>
    <td>CLIENT LIST</td>
    <td></td>
  </tr>
  <tr>
    <td>CLIENT PAUSE</td>
    <td></td>
  </tr>
  <tr>
    <td>CLIENT REPLY</td>
    <td></td>
  </tr>
  <tr>
    <td>CLIENT SETNAME</td>
    <td></td>
  </tr>
  <tr>
    <td>COMMAND</td>
    <td></td>
  </tr>
  <tr>
    <td>COMMAND COUNT</td>
    <td></td>
  </tr>
  <tr>
    <td>COMMAND GETKEYS</td>
    <td></td>
  </tr>
  <tr>
    <td>COMMAND INFO</td>
    <td></td>
  </tr>
  <tr>
    <td>CONFIG GET</td>
    <td></td>
  </tr>
  <tr>
    <td>CONFIG RESETSTAT</td>
    <td></td>
  </tr>
  <tr>
    <td>CONFIG REWRITE</td>
    <td></td>
  </tr>
  <tr>
    <td>CONFIG SET</td>
    <td></td>
  </tr>
  <tr>
    <td>DBSIZE</td>
    <td></td>
  </tr>
  <tr>
    <td>DEBUG OBJECT</td>
    <td></td>
  </tr>
  <tr>
    <td>DEBUG SEGFAULT</td>
    <td></td>
  </tr>
  <tr>
    <td>FLUSHALL</td>
    <td></td>
  </tr>
  <tr>
    <td>FLUSHDB</td>
    <td></td>
  </tr>
  <tr>
    <td>INFO</td>
    <td></td>
  </tr>
  <tr>
    <td>LASTSAVE</td>
    <td></td>
  </tr>
  <tr>
    <td>MEMORY DOCTOR</td>
    <td></td>
  </tr>
  <tr>
    <td>MEMORY HELP</td>
    <td></td>
  </tr>
  <tr>
    <td>MEMORY MALLOC-STATS</td>
    <td></td>
  </tr>
  <tr>
    <td>MEMORY PURGE</td>
    <td></td>
  </tr>
  <tr>
    <td>MEMORY STATS</td>
    <td></td>
  </tr>
  <tr>
    <td>MEMORY USAGE</td>
    <td></td>
  </tr>
  <tr>
    <td>MONITOR</td>
    <td></td>
  </tr>
  <tr>
    <td>ROLE</td>
    <td></td>
  </tr>
  <tr>
    <td>SAVE</td>
    <td></td>
  </tr>
  <tr>
    <td>SHUTDOWN</td>
    <td></td>
  </tr>
  <tr>
    <td>SLAVEOF</td>
    <td></td>
  </tr>
  <tr>
    <td>SLOWLOG</td>
    <td></td>
  </tr>
  <tr>
    <td>SYNC</td>
    <td></td>
  </tr>
  <tr>
    <td>TIME</td>
    <td></td>
  </tr>
</table>

- Sets

<table>
  <tr>
    <td>Command</td>
    <td>Support</td>
  </tr>
  <tr>
    <td>SADD</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>SCARD</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>SDIFF</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>SDIFFSTORE</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>SINTER</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>SINTERSTORE</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>SISMEMBER</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>SMEMBERS</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>SMOVE</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>SPOP</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>SRANDMEMBER</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>SREM</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>SSCAN</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>SUNION</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>SUNIONSTORE</td>
    <td>TODO</td>
  </tr>
</table>

- SortedSets

<table>
  <tr>
    <td>Command</td>
    <td>Support</td>
  </tr>
  <tr>
    <td>BZPOPMAX</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>BZPOPMIN</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZADD</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZCARD</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZCOUNT</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZINCRBY</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZINTERSTORE</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZLEXCOUNT</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZPOPMAX</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZPOPMIN</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZRANGE</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZRANGEBYLEX</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZRANGEBYSCORE</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZRANK</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZREM</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZREMRANGEBYLEX</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZREMRANGEBYRANK</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZREMRANGEBYSCORE</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZREVRANGE</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZREVRANGEBYLEX</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZREVRANGEBYSCORE</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZREVRANK</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZSCAN</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZSCORE</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>ZUNIONSTORE</td>
    <td>TODO</td>
  </tr>
</table>

- Streams

<table>
  <tr>
    <td>Command</td>
    <td>Support</td>
  </tr>
  <tr>
    <td>XADD</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>XLEN</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>XPENDING</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>XRANGE</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>XREAD</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>XREADGROUP</td>
    <td>TODO</td>
  </tr>
  <tr>
    <td>XREVRANGE</td>
    <td>TODO</td>
  </tr>
</table>

- Strings

<table>
  <tr>
    <td>Command</td>
    <td>Support</td>
  </tr>
  <tr>
    <td>APPEND</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>BITCOUNT</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>BITFIELD</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>BITOP</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>BITPOS</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>DECR</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>DECRBY</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>GET</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>GETBIT</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>GETRANGE</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>GETSET</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>INCR</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>INCRBY</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>INCRBYFLOAT</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>MGET</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>MSET</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>MSETNX</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>PSETEX</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>SET</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>SETBIT</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>SETEX</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>SETNX</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>SETRANGE</td>
    <td>Supported</td>
  </tr>
  <tr>
    <td>STRLEN</td>
    <td>Supported</td>
  </tr>
</table>

- Transaction

<table>
  <tr>
    <td>Command</td>
    <td>Support</td>
  </tr>
  <tr>
		<td>DISCARD</td>
		<td>Supported</td>
	</tr>
	<tr>
		<td>EXEC</td>
		<td>Supported</td>
	</tr>
	<tr>
		<td>MULTI</td>
		<td>Supported</td>
	</tr>
	<tr>
		<td>UNWATCH</td>
		<td>Supported</td>
	</tr>
	<tr>
		<td>WATCH</td>
		<td>Supported</td>
	</tr>
</table>
  
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

