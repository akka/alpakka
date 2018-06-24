/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc.javadsl;

import java.util.List;

public class ChangeSet {

    private long transactionId;
    private List<Change> changes;

    public ChangeSet(long transactionId, List<Change> changes) {
        this.transactionId = transactionId;
        this.changes = changes;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public List<Change> getChanges() {
        return changes;
    }


}
