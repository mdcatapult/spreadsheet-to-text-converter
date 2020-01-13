package io.mdcatapult.doclib.tabular

import java.nio.file.Paths

import org.scalatest.FlatSpec

class DocumentSpec extends FlatSpec {

  val expected = "\"Row1Cell1\"\t\"Row1Cell2\"\t\"Row1Cell3\"\n\"Row2Cell1\"\t\t\"Row2Cell3\"\n\n\"FORMULA\"\t3\t2.1\n"

  val odsSheet0 = "Row1Cell1\tRow1Cell2\tRow1Cell3\nRow2Cell1\t\tRow2Cell3\nFORMULA\t3.0\t2.1\n"
  val odsSheet1 = "Cell1Row1\tCell2Row1\tCell3Row1\nCell1Row2\tCell2Row2\tCell3Row2\nFORMULA\t6.0\t\n"

  "An XLSX file" should "be parsable as a valid document" in {
    val testFile = getClass.getResource("/test.xlsx")
    val path = Paths.get(testFile.toURI)
    val result = new Document(path) convertTo "tsv"
    assert(result.length == 2)
    assert(result.head.content == expected)
  }

  "An XLS file" should "be parsable as a valid document" in {
    val testFile = getClass.getResource("/test.xls")
    val path = Paths.get(testFile.toURI)
    val result = new Document(path) convertTo "tsv"
    assert(result.length == 2)
    assert(result.head.content == expected)
  }

  "An ODS file" should "be parsable as a valid document" in {
    val testFile = getClass.getResource("/test.ods")
    val path = Paths.get(testFile.toURI)
    val result = new Document(path) convertTo "tsv"
    assert(result.length == 2)
    assert(result.head.content == odsSheet0)
    assert(result(1).content == odsSheet1)
  }

}
