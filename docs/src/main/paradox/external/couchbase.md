# Couchbase

ReactiveCouchbase is a Scala driver that provides non-blocking and asynchronous I/O operations on top of Couchbase. ReactiveCouchbase is designed to avoid blocking on each database operations. Every operation returns immediately, using the elegant Scala Future API to resume execution when it’s over. With this driver, accessing the database is not an issue for performance anymore. ReactiveCouchbase is also highly focused on streaming data in and out from your Couchbase servers using the very nice ReactiveStreams on top of Akka Streams.

## External library

This library is not maintained in the Alpakka repository.
Learn more about it at [reactivecouchbase.org](http://reactivecouchbase.org/).
