# Welcome! Thank you for contributing to Alpakka!

We follow the standard GitHub [fork & pull](https://help.github.com/articles/using-pull-requests/#fork--pull) approach to pull requests. Just fork the official repo, develop in a branch, and submit a PR!

You're always welcome to submit your PR straight away and start the discussion (without reading the rest of this wonderful doc, or the README.md). The goal of these notes is to make your experience contributing to Alpkka as smooth and pleasant as possible. We're happy to guide you through the process once you've submitted your PR.

# The Akka Community

In case of questions about the contribution process or for discussion of specific issues please visit the [akka/dev gitter chat](https://gitter.im/akka/dev).

You may also check out these [other resources](https://akka.io/get-involved/).

# Contributing to Alpakka

## General Workflow

This is the process for committing code into master.

1. Make sure you have signed the Lightbend CLA, if not, [sign it online](http://www.lightbend.com/contribute/cla).

1. To avoid duplicated effort, it might be good to check the [issue tracker](https://github.com/akka/alpakka/issues) and [existing pull requests](https://github.com/akka/alpakka/pulls) for existing work.
   - If there is no ticket yet, feel free to [create one](https://github.com/akka/alpakka/issues/new) to discuss the problem and the approach you want to take to solve it.

1. Perform your work according to the [pull request requirements](#pull-request-requirements).

1. When the feature or fix is completed you should open a [Pull Request](https://help.github.com/articles/using-pull-requests) on [GitHub](https://github.com/akka/alpakka/pulls). Prefix your PR title with a marker to show which module it affects (eg. "JMS", or "AWS S3").

1. The Pull Request should be reviewed by other maintainers (as many as feasible/practical). Note that the maintainers can consist of outside contributors, both within and outside Lightbend. Outside contributors are encouraged to participate in the review process, it is not a closed process.

1. After the review you should fix the issues (review comments, CI failures) by pushing a new commit for new review, iterating until the reviewers give their thumbs up and CI tests pass.

1. If the branch merge conflicts with its target, rebase your branch onto the target branch.

In case of questions about the contribution process or for discussion of specific issues please visit the [akka/dev gitter chat](https://gitter.im/akka/dev).


## Alpakka specific advice

We've collected a few notes on how we would like Alpakka modules to be designed based on what has evolved so far.
Please have a look at our [contributor advice](contributor-advice.md).

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

1. [Travis CI](https://travis-ci.org/akka/alpakka) automatically merges the code, builds it, runs the tests and sets Pull Request status accordingly of results in GitHub.
1. [Scalafmt](http://scalameta.org/scalafmt/) enforces some of the code style rules.
1. [sbt-header plugin](https://github.com/sbt/sbt-header) manages consistent copyright headers in every source file.
1. The [sbt-whitesourece plugin](https://github.com/lightbend/sbt-whitesource) checks licensing models of all (transitive) dependencies. 
1. A GitHub bot checks whether you've signed the Lightbend CLA. 
