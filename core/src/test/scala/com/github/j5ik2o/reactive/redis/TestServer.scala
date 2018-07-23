package com.github.j5ik2o.reactive.redis

import java.io._
import java.net.InetSocketAddress
import java.util.UUID

import com.github.j5ik2o.reactive.redis.util.{ JarUtil, OSType }
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

sealed trait RedisMode

object RedisMode {

  case object Standalone extends RedisMode

  case object Sentinel extends RedisMode

  case object Cluster extends RedisMode

}

class TestServer(mode: RedisMode = RedisMode.Standalone, portOpt: Option[Int] = None, masterPortOpt: Option[Int] = None)
    extends RandomPortSupport {
  lazy val logger = LoggerFactory.getLogger(getClass)
  @volatile
  private[this] var process: Option[Process] = None
  private[this] val forbiddenPorts           = 6300.until(7300)
  @volatile
  private var _address: Option[InetSocketAddress] = None

  def getPort = portOpt.getOrElse(_address.get.getPort)

  def address: Option[InetSocketAddress] = _address

  val path = JarUtil
    .extractExecutableFromJar(if (OSType.ofAuto == OSType.macOS) {
      "redis-server-4.0.app"
    } else {
      "redis-server-4.0.elf"
    })
    .getPath

  private[this] def assertRedisBinaryPresent()(implicit ec: ExecutionContext): Unit = {
    val p = new ProcessBuilder(path, "--help").start()
    printlnStreamFuture(new BufferedReader(new InputStreamReader(p.getInputStream)))
    printlnStreamFuture(new BufferedReader(new InputStreamReader(p.getErrorStream)))
    p.waitFor()
    val exitValue = p.exitValue()
    require(exitValue == 0 || exitValue == 1, "redis-server binary must be present.")
  }

  private[this] def findAddress(): InetSocketAddress = {
    var tries = 100
    while (_address.isEmpty && tries >= 0) {
      _address = Some(temporaryServerAddress())
      if (forbiddenPorts.contains(_address.get.getPort)) {
        _address = None
        tries -= 1
        logger.info("try to get port...")
        Thread.sleep(5)
      }
    }
    val result = _address.getOrElse {
      sys.error("Couldn't get an address for the external redis instance")
    }
    logger.info(s"findAddress: ${_address}")
    result
  }

  protected def createConfigFile(port: Int, masterPortOpt: Option[Int]): File = {
    val f = File.createTempFile("redis-" + UUID.randomUUID().toString, ".tmp")
    f.deleteOnExit()
    var out: PrintWriter = null
    try {
      out = new PrintWriter(new BufferedWriter(new FileWriter(f)))
      val confs = Seq(
        "protected-mode no",
        s"port $port"
      ) ++ masterPortOpt.map(v => Seq(s"slaveof 127.0.0.1 $v")).getOrElse(Seq.empty)
      confs.foreach { conf =>
        out.write(conf)
        out.println()
      }
    } finally {
      if (out != null)
        out.close()
    }
    logger.info(s"createConfigFile: $f")
    f
  }

  def printlnStreamFuture(br: BufferedReader)(implicit ec: ExecutionContext): Future[Unit] = {
    val result = Future {
      br.readLine()
    }.flatMap { result =>
        if (result != null) {
          logger.debug(result)
          printlnStreamFuture(br)
        } else
          Future.successful(())
      }
      .recoverWith {
        case ex =>
          Future.successful(())
      }
    result.onComplete {
      case Success(_) =>
        br.close()
      case Failure(ex) =>
        logger.error("Occurred error", ex)
        br.close()
    }
    result
  }

  def start()(implicit ec: ExecutionContext) {
    assertRedisBinaryPresent()
    findAddress()
    logger.info("redis test server will be started")
    val port = getPort
    val conf = createConfigFile(port, masterPortOpt).getAbsolutePath
    val cmd: Seq[String] = if (mode == RedisMode.Sentinel) {
      Seq(path, conf, "--sentinel")
    } else {
      Seq(path, conf)
    }
    val builder  = new ProcessBuilder(cmd.asJava)
    val _process = builder.start()
    printlnStreamFuture(new BufferedReader(new InputStreamReader(_process.getInputStream)))
    printlnStreamFuture(new BufferedReader(new InputStreamReader(_process.getErrorStream)))
    process = Some(_process)
    Thread.sleep(200)
    logger.info("redis test server has started")
  }

  def stop(): Unit = {
    process.foreach { p =>
      logger.info("redis test server will be stopped")
      p.destroy()
      p.waitFor()
      logger.info("redis test server has stopped")
    }
  }

  def restart()(implicit ec: ExecutionContext): Unit = {
    stop()
    start()
  }

}
