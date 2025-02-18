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
import com.github.miachm.sods
import com.github.miachm.sods.SpreadSheet
import com.typesafe.config.Config
import io.mdcatapult.doclib.tabular.Sheet

import java.io.File
import scala.util.Try

class ODF(file: File) extends Parser {
  /**
   * Abstract definition to take appropriate delimiters and convert the supplied document into a list of "sheets"
   *
   * @param fieldDelimiter  String
   * @param stringDelimiter String
   * @param lineDelimiter   Option[String]
   * @return List[Sheet]
   */
  override def parse(fieldDelimiter: String, stringDelimiter: String, lineDelimiter: Option[String])(implicit system: ActorSystem, config: Config): Try[List[Sheet]] = {
    Try {
      val spreadsheet = new SpreadSheet(file)
      val sheets = for {
        sheetCount <- (0 until spreadsheet.getNumSheets).toList
      } yield createSheet(sheetCount, spreadsheet, fieldDelimiter, lineDelimiter.getOrElse("\n"))
      sheets
    }
  }

  def createSheet(sheetCount: Int, spreadsheet: SpreadSheet, fieldDelimiter: String, lineDelimiter: String): Sheet = {
    val sheet = spreadsheet.getSheet(sheetCount)

    val range: sods.Range = sheet.getDataRange

    val rowIndexes = 0 until range.getNumRows
    val columnIndexes = 0 until range.getNumColumns

    val rows = rowIndexes.foldRight(List[List[Any]]())(parseRow(range, columnIndexes))

    val rowLength = rows.map(_.length).max

    val rowsAsText = rows.map(fitRowToMaxWidth(rowLength)).map(formatRow(fieldDelimiter))

    Sheet(
      index = sheetCount,
      name = sheet.getName,
      content = rowsAsText.mkString(lineDelimiter) + lineDelimiter
    )
  }

  private def parseRow(range: sods.Range, columnIndexes: Range)(rowIndex: Int, results: List[List[Any]]): List[List[Any]] = {
    val emptyRow = List[Any]()

    val row =
      columnIndexes.foldLeft(emptyRow)((xs, columnIndex) => range.getCell(rowIndex, columnIndex).getValue :: xs)
        .dropWhile(_ == null)
        .map(x => if (x == null) "" else x)

    if (row.isEmpty) results else row :: results
  }

  private def fitRowToMaxWidth(rowLength: Int)(row: List[Any]): List[Any] =
    List.fill(rowLength - row.length)(null) ::: row

  private def formatRow(fieldDelimiter: String)(row: List[Any]): String =
    row.map(x => if (x == null) "" else x).reverse.mkString(fieldDelimiter)
}
