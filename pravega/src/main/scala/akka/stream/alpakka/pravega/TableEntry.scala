/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega

import io.pravega.client.tables.TableKey
import io.pravega.client.tables.Version

class TableEntry[+V](val tableKey: TableKey, val version: Version, val value: V)
