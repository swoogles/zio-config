package zio.config.examples

import zio.config._
import zio.{App, ExitCode, Has, ZEnv, ZIO, ZLayer, console}

import java.io.IOException

import ConfigDescriptor._
import zio.{ Console, Console }

/**
 * An example of an entire application that uses java properties
 */
final case class ApplicationConfig(bridgeIp: String, userName: String)

object ApplicationConfig {
  val configuration: ConfigDescriptor[ApplicationConfig] =
    ((string("bridgeIp")) |@| string("username"))(ApplicationConfig.apply, ApplicationConfig.unapply)
}

// The main App
object JavaPropertiesExample extends App {

  val properties = new java.util.Properties()
  properties.put("bridgeIp", "10.0.0.1")
  properties.put("username", "afs")

  override def run(args: List[String]): ZIO[ZEnv, Nothing, ExitCode] = {
    val configLayer =
      ZConfig.fromProperties(properties, ApplicationConfig.configuration, "constant")

    val pgm =
      SimpleExample.finalExecution.provideLayer(configLayer ++ ZLayer.requires[Has[Console]])

    pgm
      .foldZIO(
        throwable => Console.print(throwable.getMessage),
        _ => Console.printLine("hurray !! Application ran successfully..")
      )
      .exitCode
  }
}

// The core application functions
object SimpleExample {

  val printConfigs: ZIO[Has[ApplicationConfig] with Has[Console], IOException, Unit] =
    for {
      appConfig <- getConfig[ApplicationConfig]
      _         <- Console.printLine(appConfig.bridgeIp)
      _         <- Console.printLine(appConfig.userName)
    } yield ()

  val finalExecution: ZIO[Has[ApplicationConfig] with Has[Console], IOException, Unit] =
    for {
      _ <- printConfigs
      _ <- Console.printLine(s"processing data......")
    } yield ()
}
// A note that, with magnolia module (which is still experimental), you can skip writing the {{ configuration }} in ApplicationConfig object
// import zio.config.magnolia.DeriveConfigDescriptor_,
