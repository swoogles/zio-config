package zio.config

import zio.config.ConfigDescriptor._
import zio.config.ReadWriteRoundtripTestUtils._
import zio.config.helpers._
import zio.test.Assertion._
import zio.test._
import zio.{Has, ZIO}
import zio.Random
import zio.test.Gen

object ReadWriteRoundtripTest extends BaseSpec {

  val spec: Spec[Has[TestConfig] with Has[Random], TestFailure[String], TestSuccess] =
    suite("Coproduct support")(
      test("newtype 1 roundtrip") {
        check(genId) { p =>
          val p2 =
            for {
              written <- ZIO.fromEither(write(cId, p))
              reread  <- ZIO
                           .fromEither(
                             read(cId from ConfigSource.fromPropertyTree(written, "test", LeafForSequence.Valid))
                           )
                           .mapError(_.getMessage)
            } yield reread

          assertM(p2)(equalTo(p))
        }
      },
      test("newtype 2 roundtrip") {
        check(genDbUrl) { p =>
          val p2 =
            for {
              written <- ZIO.fromEither(write(cDbUrl, p))
              reread  <- ZIO
                           .fromEither(
                             read(cDbUrl from ConfigSource.fromPropertyTree(written, "test", LeafForSequence.Valid))
                           )
                           .mapError(_.getMessage)
            } yield reread

          assertM(p2)(equalTo(p))
        }
      },
      test("case class 1 roundtrip") {
        check(genEnterpriseAuth) { p =>
          val p2 =
            for {
              written <- ZIO.fromEither(write(cEnterpriseAuth, p))
              reread  <- ZIO
                           .fromEither(
                             read(
                               cEnterpriseAuth from ConfigSource.fromPropertyTree(written, "test", LeafForSequence.Valid)
                             )
                           )
                           .mapError(_.getMessage)
            } yield reread

          assertM(p2)(equalTo(p))
        }
      },
      test("nested case class roundtrip") {
        check(genNestedConfig) { p =>
          val p2 =
            for {
              written <- ZIO.fromEither(write(cNestedConfig, p))
              reread  <- ZIO
                           .fromEither(
                             read(
                               cNestedConfig from ConfigSource.fromPropertyTree(written, "test", LeafForSequence.Valid)
                             )
                           )
                           .mapError(_.getMessage)
            } yield reread

          assertM(p2)(equalTo(p))
        }
      },
      test("single field case class roundtrip") {
        check(genSingleField) { p =>
          val p2 =
            for {
              written <- ZIO.fromEither(write(cSingleField, p))
              reread  <-
                ZIO
                  .fromEither(
                    read(cSingleField from ConfigSource.fromPropertyTree(written, "test", LeafForSequence.Valid))
                  )
                  .mapError(_.getMessage)
            } yield reread

          assertM(p2)(equalTo(p))
        }
      },
      test("coproduct roundtrip") {
        check(genCoproductConfig) { p =>
          val p2 =
            for {
              written <- ZIO.fromEither(write(cCoproductConfig, p))
              reread  <-
                ZIO
                  .fromEither(
                    read(
                      cCoproductConfig from ConfigSource.fromPropertyTree(written, "test", LeafForSequence.Valid)
                    )
                  )
                  .mapError(_.getMessage)
            } yield reread

          assertM(p2)(equalTo(p))
        }
      },
      test("empty sequence zipped with optional nested") {
        val config = (list("a")(string) |@| nested("b")(string).optional).tupled
        val data   = (Nil, None)
        val data2  = for {
          written <- ZIO.fromEither(write(config, data))
          reread  <- ZIO
                       .fromEither(
                         read(config from ConfigSource.fromPropertyTree(written, "test", LeafForSequence.Valid))
                       )
                       .mapError(_.getMessage)
        } yield reread

        assertM(data2)(equalTo(data))
      }
    )
}

object ReadWriteRoundtripTestUtils {
  final case class CoproductConfig(coproduct: Either[DataItem, NestedPath])
  final case class DataItem(oid: Option[Id], count: Int)
  final case class EnterpriseAuth(id: Id, dburl: DbUrl)
  final case class NestedPath(enterpriseAuth: EnterpriseAuth, count: Int, factor: Float)
  final case class SingleField(count: Int)

  val genDataItem: Gen[Has[Random], DataItem] =
    for {
      oid   <- Gen.option(genId)
      count <- Gen.int
    } yield DataItem(oid, count)

  val genEnterpriseAuth: Gen[Has[Random], EnterpriseAuth] =
    for {
      id    <- genId
      dburl <- genDbUrl
    } yield EnterpriseAuth(id, dburl)

  val genNestedConfig: Gen[Has[Random], NestedPath] =
    for {
      auth   <- genEnterpriseAuth
      count  <- Gen.int
      factor <- Gen.float
    } yield NestedPath(auth, count, factor)

  val genSingleField: Gen[Has[Random], SingleField] =
    for {
      count <- Gen.int
    } yield SingleField(count)

  val genCoproductConfig: Gen[Has[Random], CoproductConfig] =
    Gen.either(genDataItem, genNestedConfig).map(CoproductConfig.apply)

  val cId: ConfigDescriptor[Id]                         = string("kId").to[Id]
  val cId2: ConfigDescriptor[Id]                        = string("kId2").to[Id]
  val cDataItem: ConfigDescriptor[DataItem]             = (cId2.optional |@| int("kDiCount")).to[DataItem]
  val cDbUrl: ConfigDescriptor[DbUrl]                   = string("kDbUrl").to[DbUrl]
  val cEnterpriseAuth: ConfigDescriptor[EnterpriseAuth] = (cId |@| cDbUrl).to[EnterpriseAuth]

  val cNestedConfig: ConfigDescriptor[NestedPath] =
    (cEnterpriseAuth |@| int("kCount") |@| float("kFactor")).to[NestedPath]

  val cSingleField: ConfigDescriptor[SingleField] =
    int("kCount").to[SingleField]

  val cCoproductConfig: ConfigDescriptor[CoproductConfig] =
    (cDataItem.orElseEither(cNestedConfig)).to[CoproductConfig]
}
