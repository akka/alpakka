## Releasing

Create a new issue from the [Alpakka Release Train Issue Template](docs/release-train-issue-template.md) and follow the steps.

### Releasing only updated docs

It is possible to release a revised documentation to the already existing release.

1. Create a new branch from a release tag. If a revised documentation is for the `v0.3` release, then the name of the new branch should be `docs/v0.3`.
1. Add and commit `version.sbt` file that pins the version to the one, that is being revised. Also set `isSnapshot` to `false` for the stable documentation links. For example:
    ```scala
    version in ThisBuild := "0.3"
    isSnapshot in ThisBuild := false
    ```
1. Make all of the required changes to the documentation.
1. Build documentation locally with `CI` settings:
    ```sh
    env CI=true sbt docs/previewSite
    ```
1. If the generated documentation looks good, send it to Gustav:
    ```sh
    env CI=true sbt docs/publishRsync
    ```
1. Do not forget to push the new branch back to GitHub.
