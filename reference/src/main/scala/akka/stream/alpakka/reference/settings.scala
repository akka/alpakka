/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.reference

// rename Java imports if the name clashes with the Scala name
import java.time.{Duration => JavaDuration}
import java.util.Optional
import java.util.function.Predicate

import scala.jdk.FunctionConverters._
import scala.jdk.OptionConverters._
import scala.concurrent.duration._

/**
 * Settings class constructor is private and not exposed as API.
 * Adding or removing arguments to methods with default values is not binary
 * compatible. However, since the constructor is private, it will be possible
 * to add or remove attributes without introducing binary incompatibilities.
 */
final class SourceSettings private (
    val clientId: String,
    val traceId: Option[String] = None,
    val authentication: Authentication = Authentication.None,
    val pollInterval: FiniteDuration = 5.seconds
) {

  def withClientId(clientId: String): SourceSettings = copy(clientId = clientId)

  /**
   * Immutable setter which can be used from both Java and Scala, even if the
   * attribute is stored in a Scala specific class.
   */
  def withTraceId(traceId: String): SourceSettings = copy(traceId = Some(traceId))

  /**
   * Separate setters for every attribute enables easy evolution of settings classes by allowing
   * deprecation and addition of attributes.
   */
  def withAuthentication(authentication: Authentication): SourceSettings = copy(authentication = authentication)

  /**
   * For attributes that uses Java or Scala specific classes, a setter is added for both APIs.
   */
  def withPollInterval(pollInterval: FiniteDuration): SourceSettings = copy(pollInterval = pollInterval)

  /**
   * Java API
   *
   * Start method documentation text with "Java API" to make it easy to notice
   * Java specific methods when browsing generated API documentation.
   */
  def withPollInterval(pollInterval: JavaDuration): SourceSettings =
    copy(pollInterval = Duration.fromNanos(pollInterval.toNanos))

  /**
   * Java API
   *
   * A separate getter for Java API that converts Scala Option to Java Optional.
   */
  def getTraceId(): Optional[String] =
    traceId.toJava

  /**
   * Java API
   *
   * A separate getter for Java API that converts Scala Duration to Java Duration.
   */
  def getPollInterval(): JavaDuration =
    JavaDuration.ofNanos(pollInterval.toNanos)

  /**
   * Private copy method for internal use only.
   */
  private def copy(clientId: String = clientId,
                   traceId: Option[String] = traceId,
                   authentication: Authentication = authentication,
                   pollInterval: FiniteDuration = pollInterval) =
    new SourceSettings(clientId, traceId, authentication, pollInterval)

  override def toString: String =
    s"SourceSettings(clientId=$clientId, traceId=$traceId, authentication=$authentication, pollInterval=$pollInterval)"
}

object SourceSettings {

  /**
   * Factory method for Scala.
   */
  def apply(clientId: String): SourceSettings = new SourceSettings(clientId)

  /**
   * Java API
   *
   * Factory method for Java.
   */
  def create(clientId: String): SourceSettings = SourceSettings(clientId)
}

/**
 * Use sealed for closed class hierarchies.
 * abstract class instead of trait for visibility inside companion object from Java.
 */
sealed abstract class Authentication
object Authentication {

  /**
   * Make singleton objects extend an abstract class with the same name.
   * This makes it possible to refer to the object type without `.type`.
   */
  sealed abstract class None extends Authentication
  case object None extends None

  /**
   * Java API
   */
  def createNone: None = None

  final class Provided private (
      verifier: String => Boolean = _ => false
  ) extends Authentication {

    def withVerifier(verifier: String => Boolean): Provided =
      copy(verifier = verifier)

    /**
     * Java API
     *
     * Because of Scala 2.11 support where Scala function is not a functional interface,
     * we need to provide a setter that accepts Java's functional interface.
     *
     * A different name is needed because after type erasure functional interfaces
     * become ambiguous in Scala 2.12.
     */
    def withVerifierPredicate(verifier: Predicate[String]): Provided =
      copy(verifier = verifier.asScala)

    private def copy(verifier: String => Boolean) =
      new Provided(verifier)

    override def toString: String =
      s"Authentication.Provided(verifier=$verifier)"
  }

  object Provided {

    /**
     * This is only accessible from Scala, because of the nested objects.
     */
    def apply(): Provided = new Provided()
  }

  /**
   * Java API
   *
   * Factory method needed to access nested object.
   */
  def createProvided(): Provided = Provided()
}
