
// load dependencies first, so that we fail early if there is a dependency problem
import $ivy.`org.openjfx:javafx-controls:17-ea+8`
import com.digitalasset.canton.version.ReleaseVersion
type test = javafx.scene.control.TableCell[String,String]

import $ivy.`org.openjfx:javafx-controls:17-ea+8`
import $ivy.`org.openjfx:javafx-base:17-ea+8`
import $ivy.`org.openjfx:javafx-fxml:17-ea+8`
import $ivy.`org.openjfx:javafx-media:17-ea+8`
import $ivy.`org.openjfx:javafx-web:17-ea+8`
import $ivy.`org.openjfx:javafx-graphics:17-ea+8`
import $ivy.`org.scalafx::scalafx:17.0.1-R26`

val version = ReleaseVersion.current.fullVersion
val (testScript, loadJar, adjustPath) = sys.props.getOrElseUpdate("demo-test", "0") match {
  case "1" => (true, false, true)
  case "2" => (true, true, false)
  case "3" => (false, false, true)
  case _ => (false, true, false)
}

// load JAR into process using ammonites magic @ and interp.load.cp
if(loadJar) {
  val jarDir = os.Path(s"demo/lib/demo_2.13-${version}.jar", base=os.pwd)
  println(s"loading jar from ${jarDir}")
  interp.load.cp(jarDir)
}

@

import com.digitalasset.canton.demo.{ReferenceDemoScript, DemoUI, DemoRunner, Notify}

// determine where the assets are
val location = sys.env.getOrElse("DEMO_ROOT", "demo")
val noPhoneHome = sys.env.keys.exists(_ == "NO_PHONE_HOME")

// start all nodes before starting the ui (the ui requires this)
val (maxWaitForPruning, bankingConnection, medicalConnection) = (ReferenceDemoScript.computeMaxWaitForPruning, banking.sequencerConnection, medical.sequencerConnection)
val loggerFactory = consoleEnvironment.environment.loggerFactory

val script = new ReferenceDemoScript(participants.all,
  bankingConnection, medicalConnection,
  location, maxWaitForPruning,
  editionSupportsPruning = consoleEnvironment.environment.isEnterprise,
  darPath = if(adjustPath) Some("./community/demo/target/scala-2.13/resource_managed/main") else None,
  additionalChecks = testScript,
  loggerFactory = loggerFactory)
if(testScript) {
  script.run()
  println("The last emperor is always the worst.")
} else {
  if(!noPhoneHome) {
    Notify.send()
  }
  val runner = new DemoRunner(new DemoUI(script, loggerFactory))
  runner.startBackground()
}