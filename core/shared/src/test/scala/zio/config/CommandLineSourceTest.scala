package zio.config

import zio.config.ReadError.{ConversionError, Step}
import zio.config.testsupport.MapConfigTestSupport.AppConfig.descriptor
import zio.config.testsupport.MapConfigTestSupport.{AppConfig, genAppConfig, stringNWithInjector}
import zio.test.Assertion._
import zio.test.environment.TestEnvironment
import zio.test.{DefaultRunnableSpec, _}
import zio.{Has, IO, ZIO}

object CommandLineSourceTest extends DefaultRunnableSpec {
  def spec: Spec[TestEnvironment, TestFailure[Nothing], TestSuccess] =
    suite("Configuration from command-line-style arguments")(
      test("Configuration from arguments roundtrip separate args --key value") {
        check(genAppConfig()) { appConfig =>
          val p2: zio.IO[ReadError[String], AppConfig] =
            for {
              args   <- toSeparateArgs(AppConfig.descriptor, appConfig)
              reread <- fromArgs(args)
            } yield reread.get

          assertM(p2.either)(isRight(equalTo(appConfig)))
        }
      },
      test("Configuration from arguments roundtrip single arg --key=value") {
        check(genAppConfig()) { appConfig =>
          val p2: zio.IO[ReadError[String], AppConfig] =
            for {
              args   <- toSingleArg(AppConfig.descriptor, appConfig)
              reread <- fromArgs(args)
            } yield reread.get

          assertM(p2.either)(isRight(equalTo(appConfig)))
        }
      },
      test("Configuration from arguments roundtrip single arg --key=value multiple values take the head") {
        check(genAppConfig()) { appConfig =>
          val p2: zio.IO[ReadError[String], AppConfig] =
            for {
              args   <- toMultiSingleArg(AppConfig.descriptor, appConfig)
              reread <- fromArgs(args)
            } yield reread.get

          assertM(p2.either)(isRight(equalTo(appConfig)))
        }
      },
      test("Configuration from arguments roundtrip singe arg --key-value where value contains = char") {
        check(genAppConfig(stringNWithInjector(1, 15, "="))) { appConfig =>
          val p2: zio.IO[ReadError[String], AppConfig] =
            for {
              args   <- toSingleArg(AppConfig.descriptor, appConfig)
              reread <- fromArgs(args)
            } yield reread.get

          assertM(p2.either)(isRight(equalTo(appConfig)))
        }
      }
    )

  def fromArgs(args: List[String]): ZIO[Any, ReadError[String], Has[AppConfig]] =
    ZIO.environment.provideLayer(ZConfig.fromCommandLineArgs(args, descriptor, Some('_'), None))

  def toSeparateArgs[A](
    descriptor: ConfigDescriptor[A],
    a: A
  ): ZIO[Any, ReadError[String], List[String]] =
    IO.fromEither(write(descriptor, a))
      .mapBoth(
        s => ConversionError[String](List(Step.Index(0)), s),
        propertyTree =>
          propertyTree.flatten.toList.flatMap { (t: (Vector[String], ::[String])) =>
            List(s"--${t._1.mkString("_")}", t._2.mkString)
          }
      )

  def toSingleArg[A](descriptor: ConfigDescriptor[A], a: A): ZIO[Any, ReadError[String], List[String]] =
    IO.fromEither(write(descriptor, a))
      .mapBoth(
        s => ConversionError[String](List(Step.Index(0)), s),
        propertyTree =>
          propertyTree.flatten.toList.flatMap { (t: (Vector[String], ::[String])) =>
            List(s"-${t._1.mkString("_")}=${t._2.mkString}")
          }
      )

  def toMultiSingleArg[A](
    descriptor: ConfigDescriptor[A],
    a: A
  ): ZIO[Any, ReadError[String], List[String]] =
    IO.fromEither(write(descriptor, a))
      .mapBoth(
        s => ConversionError[String](List(Step.Index(0)), s),
        propertyTree =>
          propertyTree.flatten.toList.flatMap { (t: (Vector[String], ::[String])) =>
            List(s"--${t._1.mkString("_")}=${t._2.mkString}") ++
              List(s"--${t._1.mkString("_")}=${t._2.mkString}") // repeated with different value (should be ignored)
          }
      )

}
