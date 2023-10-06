package qualitymetrics

import japgolly.clearconfig._
import cats.implicits._
import cats.syntax.apply._
import java.net.URL
import cats.Id

case class Credential(username: String, password: String)

object Credential {
  def config: ConfigDef[Credential] =
    ( ConfigDef.need[String]("USERNAME"),
      ConfigDef.need[String]("PASSWORD"),
    ).mapN(apply)
}

case class MyDatabaseConfig(
  port     : Int,
  url      : String,
  credential: Credential,
  schema   : Option[String])


object MyDatabaseConfig {

  def config: ConfigDef[MyDatabaseConfig] =
    (
      ConfigDef.getOrUse("PORT", 8080),
      ConfigDef.need[String]("URL"),
      Credential.config,
      ConfigDef.get[String]("SCHEMA")
    ).mapN(apply)
    .withPrefix("POSTGRES_")
}

def configSources: ConfigSources[Id] =
  ConfigSource.environment[Id] >                                             // Highest priority
  ConfigSource.propFileOnClasspath[Id]("/qualitymetrics.props", optional = true)