package com.github.j5ik2o.reactive.redis

import java.util.UUID

import com.github.j5ik2o.reactive.redis.command.CommandRequestBase
import monix.eval.Task
import org.slf4j.LoggerFactory

class RedisMasterSlavesConnection(masterConnectionPoolFactory: => RedisConnectionPool[Task],
                                  slaveConnectionPoolFactory: => RedisConnectionPool[Task])
    extends RedisConnection {

  private val logger = LoggerFactory.getLogger(getClass)

  override def id: UUID = UUID.randomUUID()

  private lazy val masterConnectionPool: RedisConnectionPool[Task] = masterConnectionPoolFactory
  private lazy val slaveConnectionPool: RedisConnectionPool[Task]  = slaveConnectionPoolFactory

  override def shutdown(): Unit = {
    masterConnectionPool.dispose()
    slaveConnectionPool.dispose()
  }

  def send[C <: CommandRequestBase](cmd: C): Task[cmd.Response] =
    if (cmd.isMasterOnly)
      masterConnectionPool.withConnectionF { con =>
        logger.debug(s"execute master command: $cmd")
        con.send(cmd)
      } else
      slaveConnectionPool.withConnectionF { con =>
        logger.debug(s"execute slave command: $cmd")
        con.send(cmd)
      }

}
