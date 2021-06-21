# Contributing to the project

## Have an Idea or Feature Request, Got a Question or just wanna Learn?

Have a problem you want the Kafka health API to solve for you?

* File a ticket on [GitHub](https://github.com/purbon/kafka-topology-builder/issues). 

## Something Not Working? Found a Bug? or a Security Issue?

If you think you found a bug, it probably is a bug.

* File it in [GitHub](https://github.com/purbon/kafka-topology-builder/issues)

# Contributing Documentation and Code Changes

If you have a bugfix or new feature that you would like to contribute, and you think it will take
more than a few minutes to produce the fix (ie; write code), it is worth discussing the change. 
You can reach us via [GitHub](https://github.com/purbon/kafka-topology-builder/issues).

Please note that Pull Requests without tests and documentation may not be merged. If you would like to contribute but do not have
experience with writing tests, please ping us and ask for help.

If you would like to contribute, but don't know where to start, you can use the GitHub labels "adoptme"
and "low hanging fruit". Issues marked with these labels are relatively easy, and provides a good starting
point to contribute.

See: https://github.com/purbon/kafka-topology-builder/labels/adoptme
https://github.com/purbon/kafka-topology-builder/labels/low%20hanging%20fruit

#### What's a CHANGELOG file?

According to [keepachangelog.com](https://keepachangelog.com/en/1.0.0/):

> A changelog is a file which contains a curated, chronologically ordered list of notable changes for each version of a project.

#### How is the CHANGELOG.md populated?

Updates are done manually, according to the following rules:

```markdown
## Unreleased
  - Removed `sleep(0.0001)` from inner loop that was making things a bit slower [#133](http://example.org)

## 3.3.3
  - Fix when no delimiter is found in a chunk, the chunk is reread - no forward progress
    is made in the file [#185](http://example.org)
```

#### What is the format of the Changelog entries?

Each entry should:

1. be a summary of the Pull Request and contain a markdown link to it.
2. start with [BREAKING], when it denotes a breaking change.
  * Note: Breaking changes should warrant a major version bump.
3. after [BREAKING], start with one of the following keywords:
    `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`.
4. keep multiple entries with the same keyword in the same changelog revision together.

The meaning of the keywords is as follows (copied from [keepachangelog.com](https://keepachangelog.com/en/1.0.0/#how):

- **`Added`** for new features.
- **`Changed`** for changes in existing functionality.
- **`Deprecated`** for soon-to-be removed features.
- **`Removed`** for now removed features.
- **`Fixed`** for any bug fixes.
- **`Security`** in case of vulnerabilities.

Example:

```
## 4.0.0
- Changed default value of `number_of_threads` from 2 to 1 [#101](http://example.org)
- Changed default value of `execution_bugs` from 30 to 0 [#104](http://example.org)
- [BREAKING] Removed obsolete option `enable_telnet` option [#100](http://example.org)

## 3.3.2
- Fixed incorrect serialization of input data when encoding was `Emacs-Mule` [#84](http://example.org)

## 3.3.1
- Fixed memory leak by removing calls to `leak_lots_of_memory` [#86](http://example.org)
```

## Contribution Steps

1. Test your changes! [Run](https://github.com/purbon/kafka-topology-builder#testing) the test suite
2. Run `mvn fmt:format` to format the code
3. Send a pull request! Push your changes to your fork of the repository and
   [submit a pull
   request](https://help.github.com/articles/using-pull-requests). In the pull
   request, describe what your changes do and mention any bugs/issues related
   to the pull request.
   
# Pull Request Guidelines

The following exists as a way to set expectations for yourself and for the review process. We *want* to merge fixes and features, so let's describe how we can achieve this:
   
## Goals

* To constantly make forward progress on PRs

* To have constructive discussions on PRs

## Overarching Guiding Principles

Keep these in mind as both authors and reviewers of PRs:

* Have empathy in both directions (reviewer <--> reviewee/author)
* Progress over perfection and personal preferences
* Authors and reviewers should proactively address questions of pacing in order to reach an acceptable balance between meeting the author's expected timeline for merging the PR and the reviewer's ability to keep up with revisions to the PR.

## As a reviewee (i.e. author) of a PR:

* I must put up atomic PRs. This helps the reviewer of the PR do a high quality review fast. "Atomic" here means two things:
  - The PR must contain related changes and leave out unrelated changes (e.g. refactorings, etc. that could be their own PR instead).
  - If the PR could be broken up into two or more PRs either "vertically" (by separating concerns logically) or horizontally (by sharding the PR into a series of PRs --- usually works well with mass refactoring or cleanup type PRs), it should. A set of such related PRs can be tracked and given context in a meta issue.

* I must strive to please the reviewer(s). In other words, bias towards taking the reviewers suggestions rather than getting into a protracted argument. This helps move the PR forward. A convenient "escape hatch" to use might be to file a new issue for a follow up discussion/PR. If you find yourself getting into a drawn out argument, ask yourself: is this a good use of our time?

## As a reviewer of a PR:

* I must first focus on whether the PR works functionally -- i.e. does it solve the problem (bug, feature, etc.) it sets out to solve.

* Then I should ask myself: can I understand what the code in this PR is doing and, more importantly, why its doing whatever its doing, within 1 or 2 passes over the PR?

  * If yes, LGTM the PR!

  * If no, ask for clarifications on the PR. This will usually lead to changes in the code such as renaming of variables/functions or extracting of functions or simply adding "why" inline comments. But first ask the author for clarifications before assuming any intent on their part.

* I must not focus on personal preferences or nitpicks. If I understand the code in the PR but simply would've implemented the same solution a different way that's great but its not feedback that belongs in the PR. Such feedback only serves to slow down progress for little to no gain.
