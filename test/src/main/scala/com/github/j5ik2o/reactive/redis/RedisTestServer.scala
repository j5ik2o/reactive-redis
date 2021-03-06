package com.github.j5ik2o.reactive.redis

import java.io._
import java.net.InetSocketAddress
import java.util.UUID

import com.github.j5ik2o.reactive.redis.util.{ JarUtil, OSType }
import org.slf4j.{ Logger, LoggerFactory }

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Var",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Serializable",
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.OptionPartial",
    "org.wartremover.warts.Recursion",
    "org.wartremover.warts.While"
  )
)
class RedisTestServer(
    mode: RedisMode = RedisMode.Standalone,
    portOpt: Option[Int] = None,
    masterPortOpt: Option[Int] = None,
    forbiddenPorts: Seq[Int] = 6300.until(7300)
) {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  @volatile private[this] var process: Option[Process]      = None
  @volatile private var _address: Option[InetSocketAddress] = None

  def getPort: Int = portOpt.getOrElse(_address.get.getPort)

  def address: Option[InetSocketAddress] = _address

  val path: String = JarUtil
    .extractExecutableFromJar(if (OSType.ofAuto == OSType.macOS) {
      "redis-server/redis-server-4.0.macOS"
    } else {
      "redis-server/redis-server-4.0.Linux"
    })
    .getPath

  private[this] def assertRedisBinaryPresent()(implicit ec: ExecutionContext): Unit = {
    val p = new ProcessBuilder(path, "--help").start()
    logStreamFuture(new BufferedReader(new InputStreamReader(p.getInputStream)), output = false)
    logStreamFuture(new BufferedReader(new InputStreamReader(p.getErrorStream)), output = false)
    p.waitFor()
    val exitValue = p.exitValue()
    require(exitValue == 0 || exitValue == 1, "redis-server binary must be present.")
  }

  private[this] def findAddress(): InetSocketAddress = {
    var tries = 100
    while (_address.isEmpty && tries >= 0) {
      _address = Some(RandomPortSupport.temporaryServerAddress())
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

  protected def logStreamFuture(br: BufferedReader, output: Boolean = true)(
      implicit ec: ExecutionContext
  ): Future[Unit] = {
    val f = Future {
      Iterator.continually(br.readLine()).takeWhile(_ != null).foreach(msg => if (output) logger.debug(msg))
    }
    f.onComplete { _ =>
      br.close()
    }
    f
  }

  def start(_port: Option[Int] = None)(implicit ec: ExecutionContext): Unit = {
    assertRedisBinaryPresent()
    findAddress()
    logger.info("redis test server will be started")
    val port = _port.getOrElse(getPort)
    val conf = createConfigFile(port, masterPortOpt).getAbsolutePath
    val cmd: Seq[String] = if (mode == RedisMode.Sentinel) {
      Seq(path, conf, "--sentinel")
    } else {
      Seq(path, conf)
    }
    val builder  = new ProcessBuilder(cmd.asJava)
    val _process = builder.start()
    Thread.sleep(200)
    logStreamFuture(new BufferedReader(new InputStreamReader(_process.getInputStream)))
    logStreamFuture(new BufferedReader(new InputStreamReader(_process.getErrorStream)))
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

  def restart(_port: Option[Int] = None)(implicit ec: ExecutionContext): Unit = {
    stop()
    start(_port)
  }

}
