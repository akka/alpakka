# Versioning 

Alpakka uses the versioning scheme `major.minor.patch` (just as the rest of the Akka family of projects). The Akka family diverges a bit from Semantic Versioning in that new, compatible functionality is added in patch releases.

@@@ note 

As all Alpakka modules (excluding Alpakka Kafka) share a single version number, modules often don't contain any notable changes even though the version number has changed significantly.

@@@

Alpakka publishes 

* regular releases to [Maven Central](https://search.maven.org/search?q=g:com.lightbend.akka%20akka-stream-)
* milestone and release candidates (of major versions) to Maven Central
* @ref:[snapshots](snapshots.md) to [Bintray](https://bintray.com/akka/snapshots/alpakka)

### Compatibility

Our ambition is to evolve Alpakka modules without breaking users’ code. There are two sides to that: One is **binary-compatibility** which effectively means you can replace just the jar with a later version’s jar in your installation and everything will work. This becomes extremely important as soon as you use other libraries that rely on the same jar. They will continue to work without recompilation. The other is **source-compatibility** which, when upgrading to a later minor version, would not require any code changes. Akka and Alpakka strive for binary-compatibility and source-compatibility, but we do not guarantee source-compatibility.

Read about the details in the @extref:[Akka documentation](akka:common/binary-compatibility-rules.html). 


### Mixing versions

**All connectors of Alpakka can be used independently**, you may mix Alpakka versions for different libraries. Some may share dependencies which could be incompatible, though (eg. Alpakka connectors to AWS services).


### Akka versions

With Akka though, it is important to be strictly using one version (never blend eg. `akka-actor 2.5.21` and `akka-stream 2.5.12`), and do not use an Akka version lower than the one the Alpakka dependency requires (sometimes Alpakka modules depend on features of the latest Akka release).

Alpakka’s Akka and Akka HTTP dependencies are upgraded only if that version brings features leveraged by Alpakka or important fixes. As Akka itself is binary-compatible, the Akka version may be upgraded with an Alpakka patch release.

@@@ note 

Users are recommended to upgrade to the latest Akka version later than the one required by Alpakka at their convenience. 

@@@


### Scala versions

The "Project information" section for every connector states which versions of Scala it is compiled with.


### Third-party dependencies

Alpakka depends heavily on third-party (non-Akka) libraries to integrate with other technologies via their client APIs. To keep up with the development within the technologies Alpakka integrates with, Alpakka connectors need to upgrade the client libraries regularly. 

Code using Alpakka will in many cases even make direct use of the client library and thus depend directly on the version Alpakka pulls into the project. Not all libraries apply the same rules for binary- and source-compatibility and it is very hard to track all of those rules. 

For this reason any library upgrade must be made with care to not break user code in unexpected ways.

* An upgrade to a *patch release of a library* may be part of an Alpakka patch release (if nothing indicates the library upgrade would break user code).
* An upgrade to a *minor release of a library* may be part of a minor Alpakka release  (if nothing indicates the library upgrade would break user code).
* For *major release upgrades of a library*, the Alpakka team will decide case-by-case if the upgrade motivates an upgrade of the Alpakka major version. 
    * For source incompatible library upgrades, Alpakka will do a major version upgrade.
    * If the library indicates it is considered binary-compatible, Alpakka may upgrade within a minor release.

If a project needs to rely on the binary-compatibility rules to just replace the Alpakka dependency, care must be taken to replace any upgraded third-party library to the version used by Alpakka at the same time. This should normally work when overriding an Alpakka dependency of a library in a build dependency definition, as the build tool will do all the necessary evictions as long as evicted libraries are backwards binary-compatible.

