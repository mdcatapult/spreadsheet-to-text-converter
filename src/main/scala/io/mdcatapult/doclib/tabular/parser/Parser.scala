package io.mdcatapult.doclib.tabular.parser

import akka.actor.ActorSystem
import akka.pattern.CircuitBreaker
import com.typesafe.config.Config
import io.mdcatapult.doclib.tabular.Sheet

import scala.concurrent.duration.DurationInt
import scala.util.Try

trait Parser {

  /**
   * Abstract definition to take appropriate delimiters and convert the supplied document into a list of "sheets"
   * @param fieldDelimiter
   * @param stringDelimiter
   * @param lineDelimiter
   * @param system
   * @param config
   * @return Try[List[Sheet]]
   */
  def parse(fieldDelimiter: String, stringDelimiter: String, lineDelimiter:Option[String] = Some("\n"))(implicit system: ActorSystem, config: Config): Try[List[Sheet]]

  /**
   * Return a circuit breaker with a timeout from config which is used when converting a spreadsheet
   * @param system
   * @param config
   * @return
   */
  def createCircuitBreaker()(implicit system: ActorSystem, config: Config): CircuitBreaker = {
    CircuitBreaker(system.scheduler, maxFailures = 1, callTimeout = config.getInt("totsv.max-timeout").milliseconds, resetTimeout = 1.minute)
  }
}