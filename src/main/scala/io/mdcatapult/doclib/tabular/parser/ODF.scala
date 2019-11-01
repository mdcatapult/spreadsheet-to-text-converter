package io.mdcatapult.doclib.tabular.parser

import java.io.File

import org.odftoolkit.odfdom.doc.OdfSpreadsheetDocument

import scala.collection.JavaConverters._
import io.mdcatapult.doclib.tabular.Sheet
import org.odftoolkit.odfdom.doc.table.OdfTable

import scala.collection.mutable

class ODF(file: File) extends Parser {
  /**
   * Abstract definition to take appropriate delimiters and convert the supplied document into a list of "sheets"
   *
   * @param fieldDelimiter  String
   * @param stringDelimiter String
   * @param lineDelimiter   Option[String]
   * @return List[Sheet]
   */
  override def parse(fieldDelimiter: String, stringDelimiter: String, lineDelimiter: Option[String]): List[Sheet] = {
    val spreadsheet = OdfSpreadsheetDocument.loadDocument(file)
    val tables = spreadsheet.getTableList.asScala
    tables(0).getColumnList.asScala.foreach(col ⇒ {
      val colList = new mutable.MutableList[String]()
      for (i <- 0 until col.getCellCount) {
        colList += col.getCellByIndex(i).getStringValue
      }
    })
    return List[Sheet]()
  }

  /**
   * Find the last member of list of tuples which satisifies a boolean function.
   * Typically used to search through a zipWithIndex list
   * @param tupleList A list of (A, Int) tuples
   * @param f A boolean function eg def notNull(a: String): Boolean = {a != ""}
   * @tparam A Type param for the tuple entry being checked
   * @tparam Int Index of the tuple list
   * @return Option[(A, Int)] The last tuple which meets the criteria in f
   */
  def findLast[A, Int](tupleList: List[(A, Int)])(f: A => Boolean): Option[(A, Int)] =
    tupleList.foldLeft(Option.empty[(A,Int)]) { (acc, cur) =>
      if (f(cur._1)) Some(cur)
      else acc
    }

  /**
   * Does the value coontain any characters or is it empty
   * ie. is there anything in the string
   * @param value The string to be checked
   * @return true or false
   */
  def notNull(value: String): Boolean =
    value != "" || value == None
}
