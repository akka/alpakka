package com.tradeshift.reaktive.akka

import akka.http.javadsl.marshalling.Marshaller

object AsyncMarshallers {
  /**
   * Helper for creating a "super-marshaller" from a number of "sub-marshallers".
   * Content-negotiation determines, which "sub-marshaller" eventually gets to do the job.
   */
  def oneOf[A, B](m1: Marshaller[A, B], m2: Marshaller[A, B]): Marshaller[A, B] = {
    Marshaller.fromScala(akka.http.scaladsl.marshalling.Marshaller.oneOf(m1.asScala, m2.asScala))
  }

}