package io.mdcatapult.doclib.tabular

package object parser {

  def escapeQuotes(text: String): String =
    text.replaceAllLiterally("\"", "\uFFFE\"")
}
