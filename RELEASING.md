## Releasing

Create a new issue from the [Alpakka Release Train Issue Template](docs/release-train-issue-template.md) and follow the steps.

### Releasing only updated docs

It is possible to release a revised documentation to the already existing release.

1. Create a new branch from a release tag. If a revised documentation is for the `v0.3` release, then the name of the new branch should be `docs/v0.3`.
2. Make all of the required changes to the documentation.
3. Add and commit `version.sbt` file that sets the version to the one, that is being revised. For example `version in ThisBuild := "0.3"`.
4. Push the branch. Tech Hub will see the new branch and will build and publish revised documentation.
