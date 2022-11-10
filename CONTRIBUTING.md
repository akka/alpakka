# Welcome! Thank you for contributing to Alpakka!

We follow the standard GitHub [fork & pull](https://help.github.com/articles/using-pull-requests/#fork--pull) approach to pull requests. Just fork the official repo, develop in a branch, and submit a PR!

You're always welcome to submit your PR straight away and start the discussion (without reading the rest of this wonderful doc, or the README.md). The goal of these notes is to make your experience contributing to Alpakka as smooth and pleasant as possible. We're happy to guide you through the process once you've submitted your PR.

# The Akka Community

In case of questions about the contribution process or for discussion of specific issues please visit the [akka/dev gitter chat](https://gitter.im/akka/dev).

You may also check out these [other resources](https://akka.io/get-involved/).

# Contributing to Alpakka

## Development Setup

Ensure to install the following dependencies:

- [Git](https://git-scm.com/) (Make sure the `git` executable is referenced in the `PATH` environment variable)
  - Having `git` in the `PATH` is important because it is used by the MiMa plugin when loading the sbt project.
- Java JDK
- [SBT](https://www.scala-sbt.org/) which can be installed standalone, or bundled with [Scala](https://scala-lang.org/) or IDEs such as [IntelliJ IDEA](https://www.jetbrains.com/idea/).

When cloning or updating the repository, make sure to clone/fetch/pull in a way which gets the git tags (such as a regular `git clone`, `git fetch` or `git pull`).
This is needed because the MiMa plugin checks the git history and tags when loading the sbt project.
Doing a shallow clone/fetch/pull will not get the tag information and will interfere with the project loading process.

## General Workflow

This is the process for committing code into master.

1. Make sure you have signed the Lightbend CLA, if not, [sign it online](https://www.lightbend.com/contribute/cla/akka).

1. To avoid duplicated effort, it might be good to check the [issue tracker](https://github.com/akka/alpakka/issues) and [existing pull requests](https://github.com/akka/alpakka/pulls) for existing work.
   - If there is no ticket yet, feel free to [create one](https://github.com/akka/alpakka/issues/new) to discuss the problem and the approach you want to take to solve it.

1. Perform your work according to the [pull request requirements](#pull-request-requirements).

1. When the feature or fix is completed you should open a [Pull Request](https://help.github.com/articles/using-pull-requests) on [GitHub](https://github.com/akka/alpakka/pulls). Prefix your PR title with a marker to show which module it affects (eg. "JMS", or "AWS S3").

1. The Pull Request should be reviewed by other maintainers (as many as feasible/practical). Note that the maintainers can consist of outside contributors, both within and outside Lightbend. Outside contributors are encouraged to participate in the review process, it is not a closed process.

1. After the review you should fix the issues (review comments, CI failures, compiler warnings) by pushing a new commit for new review, iterating until the reviewers give their thumbs up and CI tests pass.

1. If the branch merge conflicts with its target, rebase your branch onto the target branch.

In case of questions about the contribution process or for discussion of specific issues please visit the [akka/dev gitter chat](https://gitter.im/akka/dev).


## Alpakka specific advice

We've collected a few notes on how we would like Alpakka modules to be designed based on what has evolved so far.
Please have a look at our [contributor advice](contributor-advice.md).


## Binary compatibility (MiMa)

Binary compatibility rules and guarantees are described in depth in the [Binary Compatibility Rules
](https://doc.akka.io/docs/akka/snapshot/common/binary-compatibility-rules.html) section of the Akka documentation.

Akka projects use [MiMa](https://github.com/lightbend/mima) to validate binary compatibility of incoming pull requests. If your PR fails due to binary compatibility issues, you may see an error like this:

```
[info] akka-stream: found 1 potential binary incompatibilities while checking against com.typesafe.akka:akka-stream_2.12:2.4.2  (filtered 222)
[error]  * method foldAsync(java.lang.Object,scala.Function2)akka.stream.scaladsl.FlowOps in trait akka.stream.scaladsl.FlowOps is present only in current version
[error]    filter with: ProblemFilters.exclude[ReversedMissingMethodProblem]("akka.stream.scaladsl.FlowOps.foldAsync")
```

In such situations, it's good to consult with a core team member about whether the violation can be safely ignored or if it would indeed
break binary compatibility. If the violation can be ignored add exclude statements from the MiMa output to
a new file named `<module>/src/main/mima-filters/<last-version>.backwards.excludes/<pr-or-issue>-<issue-number>-<description>.excludes`,
e.g. `s3/src/main/mima-filters/1.1.x.backwards.excludes/pr-12345-rename-internal-classes.excludes`. Make sure to add a comment
in the file that describes briefly why the incompatibility can be ignored.

Situations when it may be fine to ignore a MiMa issued warning include:

- if it is touching any class marked as `private[alpakka]`, `@InternalApi`, `/** INTERNAL API*/` or similar markers
- if it is concerning internal classes (often recognisable by package names like `impl`, `internal` etc.)
- if it is adding API to classes / traits which are only meant for extension by Akka itself, i.e. should not be extended by end-users (often marked as `@DoNotInherit` or `sealed`)
- other tricky situations

The binary compatibility of the current changes can be checked by running `sbt +mimaReportBinaryIssues`.


## Pull Request Requirements

For a Pull Request to be considered at all it has to meet these requirements:

1. Pull Request branch should be given a unique descriptive name that explains its intent. Prefix your PR title with a marker to show which module it affects (eg. "JMS", or "AWS S3").

1. Refer to issues it intends to fix by adding "Fixes #{issue id}" to the notes.

1. Code in the branch should live up to the current code standard:
   - Not violate [DRY](https://www.oreilly.com/library/view/97-things-every/9780596809515/ch30.html).
   - [Boy Scout Rule](https://www.oreilly.com/library/view/97-things-every/9780596809515/ch08.html) needs to have been applied.

1. Regardless if the code introduces new features or fixes bugs or regressions, it must have comprehensive tests.

1. The code must be well documented (see the [Documentation](contributor-advice.md#documentation) section).

1. The commit messages must properly describe the changes, see [further below](#creating-commits-and-writing-commit-messages).

1. Do not use ``@author`` tags since it does not encourage [Collective Code Ownership](http://www.extremeprogramming.org/rules/collective.html). Contributors get the credit they deserve in the release notes.

If these requirements are not met then the code should **not** be merged into master, or even reviewed - regardless of how good or important it is. No exceptions.


## Creating Commits And Writing Commit Messages

Follow these guidelines when creating public commits and writing commit messages.

1. First line should be a descriptive sentence what the commit is doing. It should be possible to fully understand what the commit does — but not necessarily how it does it — by just reading this single line. We follow the “imperative present tense” style for commit messages ([more info here](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html)).

   It is **not ok** to only list the ticket number, type "minor fix" or similar.
   If the commit is a small fix, then you are done. If not, go to 3.

1. Following the single line description should be a blank line followed by an enumerated list with the details of the commit.

1. Add keywords for your commit (depending on the degree of automation we reach, the list may change over time):
    * ``Review by @gituser`` - if you want to notify someone on the team. The others can, and are encouraged to participate.

Example:

    Add eventsByTag query #123

    * Details 1
    * Details 2
    * Details 3


## How To Enforce These Guidelines?

1. [GitHub actions](https://github.com/akka/alpakka/actions) automatically merge the code, builds it, runs the tests and sets Pull Request status accordingly of results in GitHub.
1. [Scalafmt](http://scalameta.org/scalafmt/) enforces some of the code style rules.
1. [sbt-header plugin](https://github.com/sbt/sbt-header) manages consistent copyright headers in every source file.
1. A GitHub bot checks whether you've signed the Lightbend CLA. 
1. Enabling `fatalWarnings := true` for all projects.
