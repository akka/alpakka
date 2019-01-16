# IBM Bluemix Cloud Object Storage

The @ref:[Alpakka S3](s3.md) connector can connect to a range of S3 compatible services.
One of them is [IBM Bluemix Cloud Object Storage](https://ibm-public-cos.github.io/crs-docs/api-reference), which supports a dialect of the AWS S3 API.
Most functionality provided by the Alpakka S3 connector is compatible with Cloud Object Store, but there are a few limitations, which are listed below.

## Connection limitations

- The Alpakka S3 connector does not support domain-style access for Cloud Object Store, so only path-style access is supported.
- Regions in COS are always part of the host/endpoint, therefore leave the region configuration value empty
- The connection proxy, containing host/endpoint, port and scheme, must always be specified.

An example configuration for accessing Bluemix Clooud Object Storage should look like this:

application.conf
: @@snip [bluemix](/s3/src/test/resources/application.conf) { #bluemix-conf }
