/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.mock

import com.google.cloud.bigquery.storage.v1.arrow.ArrowRecordBatch
import com.google.protobuf.ByteString
import org.apache.arrow.vector.types.pojo.Schema
import java.util.Base64

object ArrowRecords {

  val GCPSerializedSchema = ByteString.copyFrom(
    Base64.getDecoder.decode("/////7AAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAIAAABQAAAABAAAAMj///8AAAECEAAAACAAAAAEAAAAAAAAAAQAAABjb2wyAAAAAAgADAAIAAcACAAAAAAAAAFAAAAAEAAUAAgABgAHAAwAAAAQABAAAAAAAAEFEAAAABwAAAAEAAAAAAAAAAQAAABjb2wxAAAAAAQABAAEAAAAAAAAAA==")
  )

  val GCPSerializedTenRecordBatch = ByteString.copyFrom(
    Base64.getDecoder.decode("/////8gAAAAUAAAAAAAAAAwAFgAGAAUACAAMAAwAAAAAAwQAGAAAABgAAAAAAAAAAAAKABgADAAEAAgACgAAAGwAAAAQAAAAAQAAAAAAAAAAAAAABQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAACAAAAAAAAAAEAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAgAAAAAAAAAAAAAAAIAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAdmFsMQAAAAACAAAAAAAAAA==")
  )

  val FullArrowSchema: Schema = Schema.fromJSON(
    """
      |{
      |  "fields" : [ {
      |    "name" : "col1",
      |    "nullable" : true,
      |    "type" : {
      |      "name" : "utf8"
      |    },
      |    "children" : [ ]
      |  }, {
      |    "name" : "col2",
      |    "nullable" : true,
      |    "type" : {
      |      "name" : "int",
      |      "bitWidth" : 64,
      |      "isSigned" : true
      |    },
      |    "children" : [ ]
      |  } ]
      |}
      |""".stripMargin)

  def fullRecordBatch(): ArrowRecordBatch = {
    ArrowRecordBatch.of(GCPSerializedTenRecordBatch,10)
  }

}
