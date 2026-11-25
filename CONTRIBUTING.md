# Contributing

Thanks for taking the time to contribute to PgVolt, an independent fork of Electric.

This guide covers two topics:

1. the [legal terms](#legal-terms) under which you contribute to the project
2. the [guidelines](#contributor-guidelines) for how to contribute to the project

## Legal terms

This project is licensed under the [Apache-2.0 License](LICENSE). Contributions are
accepted under the
[default GitHub terms](https://docs.github.com/en/site-policy/github-terms/github-terms-of-service#6-contributions-under-repository-license):
whatever you contribute to this repository is licensed under the same Apache-2.0 License.
There is no separate contributor license agreement to sign.

PgVolt is not part of the ElectricSQL project. A contribution made here is not a
contribution to upstream, and upstream's contributor license agreement does not apply to it.

## Contributor Guidelines

Fixes for bugs, security issues and performance problems in the sync service are the
contributions most likely to be merged. A new feature is off until a setting or a request asks
for it, and is documented as experimental until it settles.

Please read the guidance below about what to do if you:

- [found a bug](#did-you-find-a-bug)
- [fixed a bug](#did-you-write-a-patch-that-fixes-a-bug)
- [want to add a new feature or change an existing one](#do-you-intend-to-add-a-new-feature-or-change-an-existing-one)

### Did you find a bug?

- if it is a security vulnerability, do not open an issue: follow the [security policy](SECURITY.md)
- ensure that the bug was not already reported by searching the issues of this repository
- if you're unable to find an open issue addressing the problem, open a new one

Be sure to include a title and clear description, as much relevant information as possible, and a **code sample** or an **executable test case** demonstrating the expected behavior that is not occurring.

If the bug also reproduces on upstream Electric, say so in the issue.

### Did you write a patch that fixes a bug?

- open a new GitHub pull request with the patch
- ensure the PR description clearly describes the problem and solution. Include the relevant issue number if applicable
- add a changeset for any change to a package: a markdown file in [.changeset](.changeset) naming the package and the bump. CI checks for it

### Do you intend to add a new feature or change an existing one?

- open an issue describing the change before writing the code
- assuming you get positive feedback, raise a Pull Request to track the development of the feature and discuss the implementation
