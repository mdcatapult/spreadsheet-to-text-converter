package io.mdcatapult.doclib.tabular

import java.nio.file.Paths

import org.scalatest.FlatSpec

class DocumentSpec extends FlatSpec {

  "An XLSX file" should "be parsable as a valid document" in {
    val testFile = getClass.getResource("/test.xlsx")
    val path = Paths.get(testFile.toURI)
    val result = new Document(path) to "tsv"
    val expected = "\"Row1Cell1\"\t\"Row1Cell2\"\t\"Row1Cell3\"\n\"Row2Cell1\"\t\"Row2Cell3\"\t\"Row2Cell3\"\n\"FORMULA\"\t3\t2.1\n"
    assert(result.length == 2)
    assert(result.head.content == expected)
  }

  "An XLS file" should "be parsable as a valid document" in {
    val testFile = getClass.getResource("/test.xls")
    val path = Paths.get(testFile.toURI)
    val result = new Document(path) to "tsv"
    val expected = "\"Row1Cell1\"\t\"Row1Cell2\"\t\"Row1Cell3\"\n\"Row2Cell1\"\t\"Row2Cell3\"\t\"Row2Cell3\"\n\n\"FORMULA\"\t3\t2.1\n"
    assert(result.length == 2)
    assert(result.head.content == expected)
  }

}
