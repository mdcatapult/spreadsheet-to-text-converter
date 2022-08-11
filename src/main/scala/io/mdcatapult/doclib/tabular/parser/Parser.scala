package io.mdcatapult.doclib.tabular.parser

import akka.actor.ActorSystem
import akka.pattern.CircuitBreaker
import com.typesafe.config.Config
import io.mdcatapult.doclib.tabular.Sheet

import scala.concurrent.duration.DurationInt

trait Parser {

  /**
    * Abstract definition to take appropriate delimiters and convert the supplied document into a list of "sheets"
    * @param fieldDelimiter String
    * @param stringDelimiter String
    * @param lineDelimiter Option[String]
    * @return List[Sheet]
    */
  def parse(fieldDelimiter: String, stringDelimiter: String, lineDelimiter:Option[String] = Some("\n"))(implicit system: ActorSystem, config: Config): Option[List[Sheet]]

  def createCircuitBreaker()(implicit system: ActorSystem, config: Config): CircuitBreaker = {
    CircuitBreaker(system.scheduler, maxFailures = 1, callTimeout = config.getInt("totsv.max-timeout").milliseconds, resetTimeout = 1.minute)
  }
}
