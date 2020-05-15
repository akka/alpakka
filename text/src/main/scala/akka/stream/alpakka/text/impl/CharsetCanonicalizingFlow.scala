/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.text.impl

import java.nio.charset.{Charset, StandardCharsets}

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString

@InternalApi
private[text] class CharsetCanonicalizingFlow(outgoing: Charset) {

  // see: https://en.wikipedia.org/wiki/Byte_order_mark
  private val ensureByteOrderMark: Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].prefixAndTail(1).flatMapConcat {
      case (Seq(h), t) =>
        val bom = ByteOrderMark.UTF_8
        if (bom(0) == h(0) && bom(1) == h(1) && bom(2) == h(2)) {
          Source.single(h).concat(t)
        } else {
          Source.single(bom ++ h).concat(t)
        }
      case (h, t) => Source(h).concat(t)
    }

  private val defaultToUtf8: Exception => Charset = _ => StandardCharsets.UTF_8

  def normalizeToWindows1252(input: String): String = input match {
    // for reasons, we assume ISO_8859_1 is actually the windows encoding
    case "ISO-8859-1" | "ISO-8859-2" => "windows-1252"
    case otherwise => otherwise
  }

  def flow(lookahead: Int = 4, onError: Exception => Charset = defaultToUtf8): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].prefixAndTail(lookahead).flatMapConcat {
      case (head, tail) =>
        val detector = new com.ibm.icu.text.CharsetDetector()
        val textToDetect = head.reduce(_ ++ _).toArray
        detector.setText(textToDetect)

        val detected = try {
          val charsets = detector.detectAll()
          val bestScore = charsets(0).getConfidence
          val utfIndex = charsets.indexWhere(_.getName == "UTF-8")

          // Case where utf8 is found but its not the highest match
          val charsetName =
            if (0 < utfIndex) {
              (bestScore, charsets(utfIndex).getConfidence) match {
                // If the match is close, lets pick UTF-8
                case (best, utf8) if best - utf8 <= 10 => "UTF-8"
                case _ => normalizeToWindows1252(charsets(0).getName)
              }
            } else normalizeToWindows1252(charsets(0).getName)

          val charset = Charset.forName(charsetName)
          charset
        } catch {
          case e: Exception => onError(e)
        }

        if (detected != outgoing && outgoing == StandardCharsets.UTF_8) {
          Source(head).concat(tail).via(new CharsetTranscodingFlow(detected, outgoing)).via(ensureByteOrderMark)
        } else if (detected != outgoing) {
          Source(head).concat(tail).via(new CharsetTranscodingFlow(detected, outgoing))
        } else {
          Source(head).concat(tail)
        }
    }

}
