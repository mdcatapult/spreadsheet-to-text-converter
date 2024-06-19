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
import com.typesafe.config.Config

import java.io.File
import io.mdcatapult.doclib.tabular.Sheet
import io.mdcatapult.doclib.tabular.handlers.XlsxSheetHandler
import org.apache.poi.openxml4j.opc._
import org.apache.poi.ss.usermodel.DataFormatter
import org.apache.poi.util.XMLHelper
import org.apache.poi.xssf.eventusermodel.XSSFReader.SheetIterator
import org.apache.poi.xssf.eventusermodel._
import org.apache.poi.xssf.model.StylesTable
import org.xml.sax.InputSource

import scala.jdk.CollectionConverters._
import scala.util.Try

class XLSX(file: File) extends Parser {

  def parse(fieldDelimiter: String, stringDelimiter: String, lineDelimiter:Option[String] = Some("\n"))(implicit system: ActorSystem, config: Config): Try[List[Sheet]] = {

    Try {
      val pkg: OPCPackage = OPCPackage.open(file, PackageAccess.READ)
      val breaker = createCircuitBreaker()
        .onOpen({
          pkg.close()
        })
      breaker.withSyncCircuitBreaker({
        val reader: XSSFReader = new XSSFReader(pkg)
        val sharedStrings = new ReadOnlySharedStringsTable(pkg)
        val styles: StylesTable = reader.getStylesTable

        val it: SheetIterator = reader.getSheetsData.asInstanceOf[SheetIterator]

        val sheets = it.asScala.zipWithIndex.map(sh => {

          val contents = new StringBuilder()
          val p = XMLHelper.newXMLReader()
          p.setContentHandler(new XSSFSheetXMLHandler(
            styles,
            null,
            sharedStrings,
            new XlsxSheetHandler(contents, fieldDelimiter, stringDelimiter),
            new DataFormatter(),
            false))
          val sheetSource: InputSource = new InputSource(sh._1)

          p.parse(sheetSource)

          Sheet(
            sh._2,
            it.getSheetName,
            contents.toString()
          )
        }).toList
        pkg.close()
        sheets
      })
    }
//    catch {
//      case e: Throwable => {
//        throw e
//      }
//
//    } finally {
//      pkg.close()
//    }
  }
}
