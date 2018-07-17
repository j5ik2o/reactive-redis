package com.github.j5ik2o.reactive.redis

import java.util.UUID

import com.github.j5ik2o.reactive.redis.command.CommandRequestBase
import monix.eval.Task
import org.slf4j.LoggerFactory

class RedisMasterSlavesConnection(masterConnectionFactory: => RedisConnection,
                                  slaveConnectionPoolFactory: => RedisConnectionPool[Task])
    extends RedisConnection {

  private val logger = LoggerFactory.getLogger(getClass)

  override def id: UUID = UUID.randomUUID()

  private lazy val masterConnection: RedisConnection           = masterConnectionFactory
  private lazy val slaveConnections: RedisConnectionPool[Task] = slaveConnectionPoolFactory

  override def shutdown(): Unit = {
    masterConnection.shutdown()
    slaveConnections.dispose()
  }

  def send[C <: CommandRequestBase](cmd: C): Task[cmd.Response] =
    if (cmd.isMasterOnly) {
      logger.debug(s"execute master command: $cmd")
      masterConnection.send(cmd)
    } else
      slaveConnections.withConnectionF { con =>
        logger.debug(s"execute slave command: $cmd")
        con.send(cmd)
      }

}
