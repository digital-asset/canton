import cats.syntax.either._
import java.io.{File, FileNotFoundException}

object NetRc {

  case class LoginAndPassword(login: Option[String] = None, password: Option[String] = None)

  type Machine = String
  type Error = String
  type NetRc = Map[Machine, LoginAndPassword]

  val NetRcMachineKey = "machine"
  val NetRcLoginKey = "login"
  val NetRcPasswordKey = "password"

  /** Parses a .netrc file, limited to the `login` and `password` keys.
    */
  def parse(file: File): Either[Error, NetRc] =
    for {
      source <- Either
        .catchOnly[FileNotFoundException](scala.io.Source.fromFile(file))
        .leftMap(_.toString)
      result <-
        try {
          val content = source.mkString
          val tokens = content.split("[ \\t\\n\\x0B\\f\\r]+").toList

          tokens.length match {
            case 0 => Right(Map.empty[Machine, LoginAndPassword])
            case n if n % 2 != 0 => Left("Format error: key without value")
            case _ =>
              val firstKey = tokens.head
              if (firstKey != NetRcMachineKey)
                Left("Format error: the first key is not 'machine'")
              else
                parseNetRc(
                  tokens.tail.head,
                  tokens.tail.tail
                    .grouped(2)
                    .map {
                      case List(x, y) => (x, y)
                      case _ => sys.error("uneven size") // Should never happen
                    },
                )
          }
        } finally {
          source.close()
        }
    } yield result

  private def parseNetRc(
      firstMachine: String,
      keyValues: Iterator[(String, String)],
  ): Either[Error, NetRc] = {
    case class ParseState(machine: String, netRc: NetRc)

    val initialState = ParseState(
      firstMachine,
      Map[Machine, LoginAndPassword]().withDefaultValue(LoginAndPassword()),
    )

    val result = keyValues.foldLeft(initialState) {
      case (parseState @ ParseState(machine, netRc), (key, value)) =>
        key match {
          case NetRcMachineKey =>
            ParseState(
              value,
              netRc,
            )
          case NetRcLoginKey =>
            ParseState(
              machine,
              netRc.updated(machine, netRc(machine).copy(login = Some(value))),
            )
          case NetRcPasswordKey =>
            ParseState(
              machine,
              netRc.updated(machine, netRc(machine).copy(password = Some(value))),
            )
          case _ => parseState // We're not interested in other keys
        }
    }

    Right(result.netRc)
  }
}
