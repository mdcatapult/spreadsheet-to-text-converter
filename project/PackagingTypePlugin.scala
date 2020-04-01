import sbt._

object PackagingTypePlugin extends AutoPlugin {
  override val buildSettings: Seq[sbt.Def.Setting[_]] = {
    sys.props += "packaging.type" -> "jar"
    Nil
  }
}