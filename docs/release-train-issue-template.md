# Release Alpakka $VERSION$

<!--
# Release Train Issue Template for Alpakka

(Liberally copied and adopted from Scala itself https://github.com/scala/scala-dev/blob/b11cd2e4a4431de7867db6b39362bea8fa6650e7/notes/releases/template.md)

For every Alpakka release, make a copy of this file named after the release, and expand the variables.
Ideally replacing variables could become a script you can run on your local machine.

Variables to be expanded in this template:
- $VERSION$=???
- $BINARY_VERSION$=???

Key links:
  - akka/alpakka milestone: https://github.com/akka/alpakka/milestone/?
-->
### ~ 1 week before the release

- [ ] Check that any new `deprecated` annotations use the correct version name
- [ ] Check that open PRs and issues assigned to the milestone are reasonable
- [ ] Decide on planned release date
- [ ] Create a new milestone for the [next version](https://github.com/akka/alpakka/milestones)
- [ ] Check [closed issues without a milestone](https://github.com/akka/alpakka/issues?utf8=%E2%9C%93&q=is%3Aissue%20is%3Aclosed%20no%3Amilestone) and either assign them the 'upcoming' release milestone or `invalid/not release-bound`

### 1 day before the release

- [ ] Make sure all important / big PRs have been merged by now

### Preparing release notes in the documentation / announcement

- [ ] Check readiness levels in `/project/project-info.conf`, and put in the release date for any new modules
- [ ] For non-patch releases: Create a news item draft PR on [akka.github.com](https://github.com/akka/akka.io), using the milestone
- [ ] Move all [unclosed issues](https://github.com/akka/alpakka/issues?q=is%3Aopen+is%3Aissue+milestone%3A$VERSION$) for this milestone to the next milestone
- [ ] Close the [$VERSION$ milestone](https://github.com/akka/alpakka/milestones?direction=asc&sort=due_date)

### Cutting the release

- [ ] Wait until the [build finished](https://github.com/akka/alpakka/actions) after merging the release notes
- [ ] [Fix up the draft release](https://github.com/akka/alpakka/releases) with the next tag version `v$VERSION$`, title and release description linking to announcement and milestone
- [ ] Check that the CI release build has executed successfully (GitHub actions will start a [CI publish build](https://github.com/akka/alpakka/actions/workflows/publish.yml) for the new tag and publish artifacts to Sonatype and documentation to Gustav)

### Check availability

- [ ] Check [API](https://doc.akka.io/api/alpakka/$VERSION$/) documentation
- [ ] Check [reference](https://doc.akka.io/docs/alpakka/$VERSION$/) documentation
- [ ] Check the release on [Maven central](https://repo1.maven.org/maven2/com/lightbend/akka/akka-stream-alpakka-xml_2.12/$VERSION$/)

### When everything is on maven central
  - [ ] Log into `gustav.akka.io` as `akkarepo` 
    - [ ] update the `current` links on `repo.akka.io` to point to the latest version with
         ```
         ln -nsf $VERSION$ www/docs/alpakka/current
         ln -nsf $VERSION$ www/api/alpakka/current
         ln -nsf $VERSION$ www/docs/alpakka/$BINARY_VERSION$
         ln -nsf $VERSION$ www/api/alpakka/$BINARY_VERSION$
         ```
    - [ ] check changes and commit the new version to the local git repository
         ```
         cd ~/www
         git add docs/alpakka/$BINARY_VERSION$ docs/alpakka/current docs/alpakka/$VERSION$
         git add api/alpakka/$BINARY_VERSION$ api/alpakka/current api/alpakka/$VERSION$
         git commit -m "Alpakka $VERSION$"
         ```

### Announcements

- [ ] For non-patch releases: Merge draft news item for [akka.io](https://github.com/akka/akka.io)
- [ ] Send a release notification to [Lightbend discuss](https://discuss.akka.io)
- [ ] Tweet using the akkateam account (or ask someone to) about the new release
- [ ] Announce internally

### Afterwards

- [ ] If Couchbase has relevant changes, create/update PR in [Akka Persistence Couchbase](https://github.com/akka/akka-persistence-couchbase/) to upgrade to $VERSION$
- [ ] If Cassandra has relevant changes, create/update PR in [Akka Persistence Cassandra](https://github.com/akka/akka-persistence-cassandra/) to upgrade to $VERSION$
- [ ] Update version for [Lightbend Supported Modules](https://developer.lightbend.com/docs/lightbend-platform/introduction/getting-help/build-dependencies.html#_alpakka) in [private project](https://github.com/lightbend/lightbend-platform-docs/blob/master/docs/modules/getting-help/examples/build.sbt)
- [ ] Create an issue or PR to upgrade projects in [Alpakka Samples](https://github.com/akka/alpakka-samples)
- Close this issue
