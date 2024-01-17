# Contributing to Xline

Thanks for your help improving the project! We are so happy to have you!

There are opportunities to contribute to Xline at any level. It doesn't matter if
you are just getting started with Rust or are the most weathered expert, we can
use your help.

**No contribution is too small and all contributions are valued.**

Here are some things you can do today to get started contributing:
- Help improve the Xline documentation
- Clarify code, variables, or functions that can be renamed or commented on
- Improve test coverage

If the above suggestions don't appeal to you, you can browse the issues labeled as a `good first issue` and `help wanted`. These issues do not need a thorough understanding of the project to contribute to. The label also indicates that Xline community members have committed to providing assistance for new contributors. Another way to get started is to find a documentation improvement, such as a missing/broken link, which will give you exposure to the code submission/review process without the added complication of technical depth.

This guide will help you get started. **Do not let this guide intimidate you**.
It should be considered a map to help you navigate the process.

The [dev channel][dev] is available for any concerns not covered in this guide, please join
us!

[dev]: https://discord.gg/hqDRtYkWzm

## Conduct

The Xline project adheres to the [CNCF Community Code of Conduct][coc]. This describes
the _minimum_ behavior expected from all contributors.

[coc]: ./CODE_OF_CONDUCT.md

## Outline

* [Issues](#contributing-in-Issues)

* [Pull Requests](#pull-requests)

* [Reviewing Pull Requests](#reviewing-pull-requests)

## Contributing in Issues

For any issue, there are fundamentally three ways an individual can contribute:

1. By opening the issue for discussion: For instance, if you believe that you
   have discovered a bug in Xline, creating a new issue in [the xline-kv/Xline
   issue tracker][issue] is the way to report it.

2. By helping to triage the issue: This can be done by providing
   supporting details (a test case that demonstrates a bug), providing
   suggestions on how to address the issue, or ensuring that the issue is tagged
   correctly.

3. By helping to resolve the issue: Typically this is done either in the form of
   demonstrating that the issue reported is not a problem after all, or more
   often, by opening a Pull Request that changes some bit of something in
   Xline in a concrete and reviewable manner.

[issue]: https://github.com/xline-kv/Xline/issues

**Anybody can participate in any stage of contribution**. We urge you to
participate in the discussion around bugs and participate in reviewing PRs.

### Asking for General Help

If you have reviewed existing documentation and still have questions or are
having problems, you can [open a discussion] asking for help.

In exchange for receiving help, we ask that you contribute back a documentation
PR that helps others avoid the problems that you encountered.

[open a discussion]: https://discord.gg/hqDRtYkWzm

### Submitting a Bug Report

When opening a new issue in the Xline issue tracker, you will be presented
with a basic template that should be filled in. If you believe that you have
uncovered a bug, please fill out this form, following the template to the best
of your ability. Do not worry if you cannot answer every detail, just fill in
what you can.

The two most important pieces of information we need in order to properly
evaluate the report is a description of the behavior you are seeing and a simple
test case we can use to recreate the problem on our own. If we cannot recreate
the issue, it becomes impossible for us to fix.

In order to rule out the possibility of bugs introduced by userland code, test
cases should be limited, as much as possible, to using only Xline APIs.

See [How to create a Minimal, Complete, and Verifiable example][mcve].

[mcve]: https://stackoverflow.com/help/mcve

### Triaging a Bug Report

Once an issue has been opened, it is not uncommon for there to be discussion
around it. Some contributors may have differing opinions about the issue,
including whether the behavior being seen is a bug or a feature. This discussion
is part of the process and should be kept focused, helpful, and professional.

Short, clipped responses—that provide neither additional context nor supporting
detail—are not helpful or professional. To many, such responses are simply
annoying and unfriendly.

Contributors are encouraged to help one another make forward progress as much as
possible, empowering one another to solve issues collaboratively. If you choose
to comment on an issue that you feel either is not a problem that needs to be
fixed, or if you encounter information in an issue that you feel is incorrect,
explain why you feel that way with additional supporting context, and be willing
to be convinced that you may be wrong. By doing so, we can often reach the
correct outcome much faster.

### Resolving a Bug Report

In the majority of cases, issues are resolved by opening a Pull Request. The
process for opening and reviewing a Pull Request is similar to that of opening
and triaging issues, but carries with it a necessary review and approval
workflow that ensures that the proposed changes meet the minimal quality and
functional guidelines of the Xline project.

## Pull Requests

Pull Requests are the way concrete changes are made to the code, documentation,
and dependencies in the Xline repository.

Even tiny pull requests (e.g., one character pull request fixing a typo in API
documentation) are greatly appreciated. Before making a large change, it is
usually a good idea to first open an issue describing the change to solicit
feedback and guidance. This will increase the likelihood of the PR getting
merged.

### Self-tests

To ensure the quality of the project, please run the following commands before
submitting the PR, and make sure there are no problems. Otherwise, the submitted PR will fail the automatic check.

Ensure that all tests are passed after code changes.

```rust
cargo test
```

Make sure there is no violation of clippy rules. Sometimes we need to allow
certain rules, so make sure there is a good reason. You can allow them like :
`#[allow(clippy::as_conversions)]`.

```rust
cargo clippy --all-features --all-targets -- -D warnings
```

Check the code format.

```rust
cargo fmt --all -- --check
```

**Note**: The current version of the Rust toolchain used for CI is **1.71.0** .
Please use this version to run the above command, otherwise you may get
different results with CI.

### Tests

If the change being proposed alters code (as opposed to only documentation for
example), it is either adding new functionality to Xline or it is fixing
existing, broken functionality. In both of these cases, the pull request should
include one or more tests to ensure that Xline does not regress in the future.
There are two ways to write tests: integration tests and documentation tests
(In most cases, Xline needs end-to-end integration testing, and contributors
can add corresponding unit tests according to the situation. Since all #[test]-related
code should be excluded from coverage, please use the [coverage-helper](https://github.com/taiki-e/coverage-helper)
cate to write unit tests.)

#### Integration tests

Integration tests go in the `xline/tests` and the `curp/tests`. The best strategy
for writing a new integration test is to look at existing integration tests in the
crate and follow the style.

#### Documentation tests

Ideally, every API has at least one [documentation test] that demonstrates how to
use the API. Documentation tests are run with `cargo test --doc`. This ensures
that the example is correct and provides additional test coverage.

The trick to documentation tests is striking a balance between being succinct
for a reader to understand and actually testing the API.

Same as with integration tests, when writing a documentation test, you can look at
existing doc tests in `lib.rs` and follow the style.

### Commits

It is a recommended best practice to keep your changes as logically grouped as
possible within individual commits. There is no limit to the number of commits
any single Pull Request may have, and many contributors find it easier to review
changes that are split across multiple commits.

That said, if you have a number of commits that are "checkpoints" and don't
represent a single logical change, please squash those together.

Note that multiple commits often get squashed when they are landed (see the
notes about [commit squashing](#commit-squashing)).

#### Commit message guidelines

A good commit message should describe what changed and why. The Xline project
uses conventional commits to automatically generating CHANGELOGs.

Read the [docs](https://www.conventionalcommits.org) for more details.

Note: Currently, the scope in the commit message is optional. Different projects
may have different scopes. In order to standardize the scope range, Xline currently
defines four scopes: curp, xline, persistent, and test.

### Opening the Pull Request

Explain the context and why you're making that change. What is the problem
you're trying to solve?

Summarize the solution and provide any necessary context needed to understand
the code change.

### Discuss and update

You will probably get feedback or requests for changes to your Pull Request.
This is a big part of the submission process so don't be discouraged! Some
contributors may sign off on the Pull Request right away, others may have
more detailed comments or feedback. This is a necessary part of the process
in order to evaluate whether the changes are correct and necessary.

**Any community member can review a PR and you might get conflicting feedback**.
Keep an eye out for comments from code owners to provide guidance on conflicting
feedback.

**Once the PR is open, do not rebase the commits**. See [Commit Squashing](#commit-squashing) for
more details.

### Commit Squashing

In most cases, **do not squash commits that you add to your Pull Request during
the review process**. When the commits in your Pull Request land, they may be
squashed into one commit per logical change. Metadata will be added to the
commit message (including links to the Pull Request, links to relevant issues,
and the names of the reviewers). The commit history of your Pull Request,
however, will stay intact on the Pull Request page.

## Reviewing Pull Requests

**Any Xline community member is welcome to review any pull request**.

All Xline contributors who choose to review and provide feedback on Pull
Requests have a responsibility to both the project and the individual making the
contribution. Reviews and feedback must be helpful, insightful, and geared
towards improving the contribution as opposed to simply blocking it. If there
are reasons why you feel the PR should not land, explain what those are. Do not
expect to be able to block a Pull Request from advancing simply because you say
"No" without giving an explanation. Be open to having your mind changed. Be open
to working with the contributor to make the Pull Request better.

Reviews that are dismissive or disrespectful of the contributor or any other
reviewers are strictly counter to the Code of Conduct.

When reviewing a Pull Request, the primary goals are for the codebase to improve
and for the person submitting the request to succeed. **Even if a Pull Request
does not land, the submitters should come away from the experience feeling like
their effort was not wasted or unappreciated**. Every Pull Request from a new
contributor is an opportunity to grow the community.

### Review a bit at a time

Do not overwhelm new contributors.

It is tempting to micro-optimize and make everything about relative performance,
perfect grammar, or exact style matches. Do not succumb to that temptation.

Focus first on the most significant aspects of the change:

1. Does this change make sense for Xline?
2. Does this change make Xline better, even if only incrementally?
3. Are there clear bugs or larger scale issues that need attending to?
4. Is the commit message readable and correct? If it contains a breaking change
   is it clear enough?

Note that only **incremental** improvement is needed to land a PR. This means
that the PR does not need to be perfect, only better than the status quo. Follow
up PRs may be opened to continue iterating.

When changes are necessary, _request_ them, do not _demand_ them, and **do not
assume that the submitter already knows how to add a test or run a benchmark**.

Specific performance optimization techniques, coding styles and conventions
change over time. The first impression you give to a new contributor never does.

Nits (requests for small changes that are not essential) are fine, but try to
avoid stalling the Pull Request. Most nits can typically be fixed by the Xline
Collaborator landing the Pull Request but they can also be an opportunity for
the contributor to learn a bit more about the project.

It is always good to clearly indicate nits when you comment: e.g.
`Nit: change foo() to bar(). But this is not blocking.`

If your comments were addressed but were not folded automatically after new
commits or if they proved to be mistaken, please, [hide them][hiding-a-comment]
with the appropriate reason to keep the conversation flow concise and relevant.

### Be aware of the person behind the code

Be aware that _how_ you communicate requests and reviews in your feedback can
have a significant impact on the success of the Pull Request. Yes, we may land
a particular change that makes Xline better, but the individual might just not
want to have anything to do with Xline ever again. The goal is not just having
good code.

### Abandoned or Stalled Pull Requests

If a Pull Request appears to be abandoned or stalled, it is polite to first
check with the contributor to see if they intend to continue the work before
checking if they would mind if you took it over (especially if it just has nits
left). When doing so, it is courteous to give the original contributor credit
for the work they started (either by preserving their name and email address in
the commit log, or by using an `Author:` meta-data tag in the commit.

_Adapted from the [Tokio contributing guide][tokio]_.

[tokio]: https://github.com/tokio-rs/tokio/blob/master/CONTRIBUTING.md
[hiding-a-comment]: https://help.github.com/articles/managing-disruptive-comments/#hiding-a-comment
[documentation test]: https://doc.rust-lang.org/rustdoc/documentation-tests.html
