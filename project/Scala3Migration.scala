import sbt.{Def, Keys}

object Scala3Migration {
  def onScalaVersion[T](
      shared: Seq[T] = Seq.empty,
      scala213: Seq[T] = Seq.empty,
      scala3: Seq[T] = Seq.empty,
  ): Def.Initialize[Seq[T]] =
    Def.setting {
      Keys.scalaBinaryVersion.value match {
        case "2.13" => shared ++ scala213
        case "3" => shared ++ scala3
        case v => sys.error(s"unexpected Scala binary version: $v")
      }
    }
}
