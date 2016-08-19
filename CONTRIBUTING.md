# Contributing to Akka Stream Contrib

In case of questions about the contribution process or for discussion of specific issues please visit the [akka/dev gitter chat](https://gitter.im/akka/dev).

## Infrastructure

* [Lightbend Contributor License Agreement](http://www.lightbend.com/contribute/cla)
* [Issue Tracker](https://github.com/akka/akka-stream-contrib/issues)
* [CI](https://travis-ci.org/akka/akka-stream-contrib)

# Lightbend Project & Developer Guidelines

These guidelines are meant to be a living document that should be changed and adapted as needed. We encourage changes that makes it easier to achieve our goals in an efficient way.

These guidelines mainly apply to Lightbend’s “mature” projects - not necessarily to projects of the type ‘collection of scripts’ etc.

## Branches summary

Depending on which version (or sometimes module) you want to work on, you should target a specific branch as explained below:

* `master` – development branch

## General Workflow

This is the process for committing code into master. There are of course exceptions to these rules, for example minor changes to comments and documentation, fixing a broken build etc.

1. Make sure you have signed the Lightbend CLA, if not, [sign it online](http://www.lightbend.com/contribute/cla).
2. Before starting to work on a feature or a fix, make sure that:
    1. There is a ticket for your work in the project's issue tracker. If not, create it first.
    2. The ticket has been scheduled for the current milestone.
    3. The ticket have been discussed and prioritized by the team.
3. You should always perform your work in a Git feature branch. The branch should be given a descriptive name that explains its intent. Some teams also like adding the ticket number and/or the [GitHub](http://github.com) user ID to the branch name, these details is up to each of the individual teams.

    akka-stream-contrib prefers the committer name as part of the branch name, the ticket number is optional.

4. When the feature or fix is completed you should open a [Pull Request](https://help.github.com/articles/using-pull-requests) on GitHub.
5. The Pull Request should be reviewed by other maintainers (as many as feasible/practical). Note that the maintainers can consist of outside contributors, both within and outside Lightbend. Outside contributors (for example from EPFL or independent committers) are encouraged to participate in the review process, it is not a closed process.
6. After the review you should fix the issues as needed (pushing a new commit for new review etc.), iterating until the reviewers give their thumbs up.

    When the branch conflicts with its merge target (either by way of git merge conflict or failing CI tests), do **not** merge the target branch into your feature branch. Instead rebase your branch onto the target branch. Merges complicate the git history, especially for the squashing which is necessary later (see below).

7. Once the code has passed review the Pull Request can be merged into the master branch. For this purpose the commits which were added on the feature branch should be squashed into a single commit. This can be done using the command `git rebase -i master` (or the appropriate target branch), `pick`ing the first commit and `squash`ing all following ones.

    Also make sure that the commit message conforms to the syntax specified below.

8. If the code change needs to be applied to other branches as well, create pull requests against those branches which contain the change after rebasing it onto the respective branch and await successful verification by the continuous integration infrastructure; then merge those pull requests.

    Please mark these pull requests with `(for validation)` in the title to make the purpose clear in the pull request list.

9. Once everything is said and done, associate the ticket with the “earliest” release milestone (i.e. if back-ported so that it will be in release x.y.z, find the relevant milestone for that release) and close it.

## Pull Request Requirements

For a Pull Request to be considered at all it has to meet these requirements:

1. Live up to the current code standard:
   - Not violate [DRY](http://programmer.97things.oreilly.com/wiki/index.php/Don%27t_Repeat_Yourself).
   - [Boy Scout Rule](http://programmer.97things.oreilly.com/wiki/index.php/The_Boy_Scout_Rule) needs to have been applied.
2. Regardless if the code introduces new features or fixes bugs or regressions, it must have comprehensive tests.
3. The code must be well documented in the Lightbend's standard documentation format (see the ‘Documentation’ section below).
4. The commit messages must properly describe the changes, see further below.
5. All Lightbend projects must include Lightbend copyright notices.  Each project can choose between one of two approaches:

    1. All source files in the project must have a Lightbend copyright notice in the file header.
    2. The Notices file for the project includes the Lightbend copyright notice and no other files contain copyright notices.  See http://www.apache.org/legal/src-headers.html for instructions for managing this approach for copyrights.

    akka-stream-contrib uses the first choice, having copyright notices in every file header.

    Other guidelines to follow for copyright notices:

    - Use a form of ``Copyright (C) 2011-2015 Lightbend Inc. <http://www.lightbend.com>``, where the start year is when the project or file was first created and the end year is the last time the project or file was modified.
    - Never delete or change existing copyright notices, just add additional info.
    - Do not use ``@author`` tags since it does not encourage [Collective Code Ownership](http://www.extremeprogramming.org/rules/collective.html). However, each project should make sure that the contributors gets the credit they deserve—in a text file or page on the project website and in the release notes etc.

If these requirements are not met then the code should **not** be merged into master, or even reviewed - regardless of how good or important it is. No exceptions.

Whether or not a pull request (or parts of it) shall be back- or forward-ported will be discussed on the pull request discussion page, it shall therefore not be part of the commit messages. If desired the intent can be expressed in the pull request description.

## Continuous Integration

Each project should be configured to use a continuous integration (CI) tool (i.e. a build server à la Jenkins). Lightbend has a [Jenkins server farm](https://jenkins.akka.io/) that can be used. The CI tool should, on each push to master, build the **full** distribution and run **all** tests, and if something fails it should email out a notification with the failure report to the committer and the core team. The CI tool should also be used in conjunction with a Pull Request validator (discussed below).

## Documentation

All documentation should be in Scaladoc and/or Javadoc.

## External Dependencies

All the external runtime dependencies for the project, including transitive dependencies, must have an open source license that is equal to, or compatible with, [Apache 2](http://www.apache.org/licenses/LICENSE-2.0).

This must be ensured by manually verifying the license for all the dependencies for the project:

1. Whenever a committer to the project changes a version of a dependency (including Scala) in the build file.
2. Whenever a committer to the project adds a new dependency.
3. Whenever a new release is cut (public or private for a customer).

Which licenses are compatible with Apache 2 are defined in [this doc](http://www.apache.org/legal/3party.html#category-a), where you can see that the licenses that are listed under ``Category A`` automatically compatible with Apache 2, while the ones listed under ``Category B`` needs additional action:

> Each license in this category requires some degree of [reciprocity](http://www.apache.org/legal/3party.html#define-reciprocal); therefore, additional action must be taken in order to minimize the chance that a user of an Apache product will create a derivative work of a reciprocally-licensed portion of an Apache product without being aware of the applicable requirements.

Each project must also create and maintain a list of all dependencies and their licenses, including all their transitive dependencies. This can be done in either in the documentation or in the build file next to each dependency.

## Work In Progress

It is ok to work on a public feature branch in the GitHub repository. Something that can sometimes be useful for early feedback etc. If so then it is preferable to name the branch accordingly. This can be done by either prefix the name with ``wip-`` as in ‘Work In Progress’, or use hierarchical names like ``wip/..``, ``feature/..`` or ``topic/..``. Either way is fine as long as it is clear that it is work in progress and not ready for merge. This work can temporarily have a lower standard. However, to be merged into master it will have to go through the regular process outlined above, with Pull Request, review etc..

Also, to facilitate both well-formed commits and working together, the ``wip`` and ``feature``/``topic`` identifiers also have special meaning.   Any branch labelled with ``wip`` is considered “git-unstable” and may be rebased and have its history rewritten.   Any branch with ``feature``/``topic`` in the name is considered “stable” enough for others to depend on when a group is working on a feature.

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

### Make Use of Pull Request Validator
akka-stream-contrib uses [Travis pull request builder](https://travis-ci.org/akka/akka-stream-contrib)
that automatically merges the code, builds it, runs the tests and comments on the Pull Request in GitHub.

## Source style

akka-stream-contrib uses [Scalariform](https://github.com/daniel-trinh/scalariform) to enforce some of the code style rules.

## Releasing

1. Create a [new release](https://github.com/akka/akka-stream-contrib/releases/new) with the next tag version (e.g. `v0.3`), title and release decsription including notable changes.
2. Fetch tags to your local clone: `git fetch --tags`.
3. Publish signed release: `sbt publishSigned`.
4. Stage and close [sonatype repository](https://oss.sonatype.org/).
