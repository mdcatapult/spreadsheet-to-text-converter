package io.mdcatapult.doclib.tabular

import java.nio.file.Paths

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DocumentSpec extends AnyFlatSpec with Matchers {

  val expected = "\"Row1Cell1\"\t\"Row1Cell2\"\t\"Row1Cell3\"\n\"Row2Cell1\"\t\t\"Row2Cell3\"\n\n\"FORMULA\"\t3\t2.1\n\"Cell with \\\"quoted\\\" text\"\n"

  val odsSheet0 = "Row1Cell1\tRow1Cell2\tRow1Cell3\nRow2Cell1\t\tRow2Cell3\nFORMULA\t3.0\t2.1\nCell with \"quoted\" text\t\t\n"
  val odsSheet1 = "Cell1Row1\tCell2Row1\tCell3Row1\nCell1Row2\tCell2Row2\tCell3Row2\nFORMULA\t6.0\t\n"

  private def convert(testFileName: String): List[Sheet] = {
    val testFile = getClass.getResource(testFileName)
    val path = Paths.get(testFile.toURI)

    new Document(path).convertTo("tsv")
  }

  "A Document" should "parse an XSLX document" in {
    val result = convert("/test.xlsx")

    result should have length 2
    result.head.content should be (expected)
  }

  it should "parse an XLS document" in {
    val result = convert("/test.xls")

    result should have length 2
    result.head.content should be (expected)
  }

  it should "parse an ODS document" in {
    val result = convert("/test.ods")

    result should have length 2
    result.head.content should be (odsSheet0)
    result(1).content should be (odsSheet1)
  }

  it should "parse a CSV document" in {
    val result = convert("/test.csv")

    result should have length 1
    result.head.content should be (odsSheet0.replaceAll("\t\t\n$", ""))
  }

  it should "parse an XLSX document that has an .xls extension" in {
    val result = convert("/misnamed-xslx-test.xls")

    result should have length 2
    result.head.content should be (expected)
  }

  it should "parse an XLS document that has an .xlsx extension" in {
    val result = convert("/misnamed-xsl-test.xlsx")

    result should have length 2
    result.head.content should be (expected)
  }

}
