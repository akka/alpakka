## Releasing

1. Create a [new release](https://github.com/akka/alpakka/releases/new) with the next tag version (e.g. `v0.3`), title and release description including notable changes mentioning external contributors.
2. Travis CI will start a [CI build](https://travis-ci.org/akka/alpakka/builds) for the new tag and publish artifacts to Bintray.
3. Login to [Bintray](https://bintray.com/akka/maven/alpakka) and sync artifacts to Maven Central.

### Releasing only updated docs

It is possible to release a revised documentation to the already existing release.

1. Create a new branch from a release tag. If a revised documentation is for the `v0.3` release, then the name of the new branch should be `docs/v0.3`.
2. Make all of the required changes to the documentation.
3. Add and commit `version.sbt` file that sets the version to the one, that is being revised. For example `version in ThisBuild := "0.1"`.
4. Push the branch. Tech Hub will see the new branch and will build and publish revised documentation.
