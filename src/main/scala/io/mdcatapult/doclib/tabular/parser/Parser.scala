/*
 * Copyright 2024 Medicines Discovery Catapult
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mdcatapult.doclib.tabular.parser

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.pattern.CircuitBreaker
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