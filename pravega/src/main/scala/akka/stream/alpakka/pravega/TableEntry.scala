/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega

import io.pravega.client.tables.TableKey
import io.pravega.client.tables.Version

class TableEntry[+V](val tableKey: TableKey, version: Version, val value: V)
