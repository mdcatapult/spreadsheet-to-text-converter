package io.mdcatapult.doclib.tabular.parser

import akka.actor.ActorSystem
import io.mdcatapult.doclib.tabular.Sheet

trait Parser {

  /**
    * Abstract definition to take appropriate delimiters and convert the supplied document into a list of "sheets"
    * @param fieldDelimiter String
    * @param stringDelimiter String
    * @param lineDelimiter Option[String]
    * @return List[Sheet]
    */
  def parse(fieldDelimiter: String, stringDelimiter: String, lineDelimiter:Option[String] = Some("\n"))(implicit system: ActorSystem): Option[List[Sheet]]
}
