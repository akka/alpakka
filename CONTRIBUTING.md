# Contributing to Alpakka

## General Workflow

This is the process for committing code into master.

1. Make sure you have signed the Lightbend CLA, if not, [sign it online](http://www.lightbend.com/contribute/cla).
2. Before starting to work on a feature or a fix, make sure that there is a ticket for your work in the [issue tracker](https://github.com/akka/alpakka/issues). If not, create it first.
3. Perform your work according to the [pull request requirements](#pull-request-requirements).
4. When the feature or fix is completed you should open a [Pull Request](https://help.github.com/articles/using-pull-requests) on [GitHub](https://github.com/akka/alpakka/pulls).
5. The Pull Request should be reviewed by other maintainers (as many as feasible/practical). Note that the maintainers can consist of outside contributors, both within and outside Lightbend. Outside contributors are encouraged to participate in the review process, it is not a closed process.
6. After the review you should fix the issues (review comments, CI failures) by pushing a new commit for new review, iterating until the reviewers give their thumbs up and CI tests pass.
7. If the branch merge conflicts with its target, rebase your branch onto the target branch.

In case of questions about the contribution process or for discussion of specific issues please visit the [akka/dev gitter chat](https://gitter.im/akka/dev).

## Testing

Some tests (cassandra, rabbitmq, mqtt, dynamodb) require a server to be running. A set of servers can easily be started with `docker-compose up`.

## Pull Request Requirements

For a Pull Request to be considered at all it has to meet these requirements:

1. Pull Request branch should be given a unique descriptive name that explains its intent.
2. Code in the branch should live up to the current code standard:
   - Not violate [DRY](http://programmer.97things.oreilly.com/wiki/index.php/Don%27t_Repeat_Yourself).
   - [Boy Scout Rule](http://programmer.97things.oreilly.com/wiki/index.php/The_Boy_Scout_Rule) needs to have been applied.
3. Regardless if the code introduces new features or fixes bugs or regressions, it must have comprehensive tests.
4. The code must be well documented (see the [Documentation](#documentation) section below).
5. The commit messages must properly describe the changes, see [further below](#creating-commits-and-writing-commit-messages).
6. Do not use ``@author`` tags since it does not encourage [Collective Code Ownership](http://www.extremeprogramming.org/rules/collective.html). Contributors get the credit they deserve in the release notes.

If these requirements are not met then the code should **not** be merged into master, or even reviewed - regardless of how good or important it is. No exceptions.

## Documentation

Documentation should be written in two forms:

1. API documentation in the form of scaladoc/javadoc comments on the Scala and Java user API.
2. Guide documentation in [docs](docs/) subproject using [Paradox](https://github.com/lightbend/paradox) documentation tool. This documentation should give a short introduction of how a given connector should be used.

Run `sbt docs/local:paradox` to generate reference docs while developing. Generated documentation can be found in the `./docs/target/paradox/site/local` directory.

## External Dependencies

All the external runtime dependencies for the project, including transitive dependencies, must have an open source license that is equal to, or compatible with, [Apache 2](http://www.apache.org/licenses/LICENSE-2.0).

This must be ensured by manually verifying the license for all the dependencies for the project:

1. Whenever a committer to the project changes a version of a dependency (including Scala) in the build file.
2. Whenever a committer to the project adds a new dependency.
3. Whenever a new release is cut (public or private for a customer).

Every external dependency listed in the build file must have a trailing comment with the license name of the dependency.

Which licenses are compatible with Apache 2 are defined in [this doc](http://www.apache.org/legal/3party.html#category-a), where you can see that the licenses that are listed under ``Category A`` automatically compatible with Apache 2, while the ones listed under ``Category B`` needs additional action:

> Each license in this category requires some degree of [reciprocity](http://www.apache.org/legal/3party.html#define-reciprocal); therefore, additional action must be taken in order to minimize the chance that a user of an Apache product will create a derivative work of a reciprocally-licensed portion of an Apache product without being aware of the applicable requirements.

## Creating Commits And Writing Commit Messages

Follow these guidelines when creating public commits and writing commit messages.

1. If your work spans multiple local commits (for example; if you do safe point commits while working in a feature branch or work in a branch for long time doing merges/rebases etc.) then please do not commit it all but rewrite the history by squashing the commits into a single big commit which you write a good commit message for (like discussed in the following sections). For more info read this article: [Git Workflow](http://sandofsky.com/blog/git-workflow.html). Every commit should be able to be used in isolation, cherry picked etc.

2. First line should be a descriptive sentence what the commit is doing, including the ticket number. It should be possible to fully understand what the commit does—but not necessarily how it does it—by just reading this single line. We follow the “imperative present tense” style for commit messages ([more info here](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html)).

   It is **not ok** to only list the ticket number, type "minor fix" or similar.
   If the commit is a small fix, then you are done. If not, go to 3.

3. Following the single line description should be a blank line followed by an enumerated list with the details of the commit.

4. Add keywords for your commit (depending on the degree of automation we reach, the list may change over time):
    * ``Review by @gituser`` - if you want to notify someone on the team. The others can, and are encouraged to participate.

Example:

    Add eventsByTag query #123

    * Details 1
    * Details 2
    * Details 3

## How To Enforce These Guidelines?

1. [Travis CI](https://travis-ci.org/akka/alpakka) automatically merges the code, builds it, runs the tests and sets Pull Request status accordingly of results in GitHub.
2. [Scalafmt](https://olafurpg.github.io/scalafmt) enforces some of the code style rules.
3. [sbt-header plugin](https://github.com/sbt/sbt-header) manages consistent copyright headers in every source file.
