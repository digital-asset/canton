import scala.xml.{Node => XmlNode, _}
import scala.xml.transform.{RewriteRule, RuleTransformer}
import sbt.Keys._
import sbt._

object PomUtils {

  /** SBT setting to remove a specific artifact from the generated POM file.
    * This method defines a `pomPostProcess` rule that removes any dependency
    * with the artifactId passed as a parameter.
    *
    * To use this, add the following to your build.sbt:
    *
    * pomPostProcess := PomUtils.removeArtifactFromPom("artifact-id-to-remove")
    *
    * @param artifactIdToRemove The artifactId of the dependency to remove.
    * @return A function that takes an XML Node (the POM) and returns a transformed XML Node.
    */
  def removeArtifactFromPom(artifactIdToRemove: String): XmlNode => XmlNode = {
    // Define a RewriteRule to transform the XML
    val rule = new RewriteRule {
      override def transform(n: XmlNode): Seq[XmlNode] = n match {
        // Match a <dependency> element
        case elem @ Elem(prefix, "dependency", attribs, scope, children @ _*) =>
          // Check if any child of the dependency is an <artifactId> with the target name
          val hasTargetArtifactId = children.exists {
            case Elem(_, "artifactId", _, _, dependencyChildren @ _*) =>
              dependencyChildren.exists(_.text == artifactIdToRemove)
            case _ => false
          }
          // If the dependency contains the target artifactId, remove it by returning an empty sequence
          if (hasTargetArtifactId) NodeSeq.Empty
          // Otherwise, keep the dependency as is
          else Elem(prefix, "dependency", attribs, scope, minimizeEmpty = false, children: _*)
        // For any other element, keep it as is, applying transformations to its children
        case other: Elem => other.copy(child = other.child.flatMap(transform))
        case other => other
      }
    }
    // Return a function that applies the rule using a RuleTransformer
    new RuleTransformer(rule)
  }
}
