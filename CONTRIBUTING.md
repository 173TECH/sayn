# Contributing To SAYN

We want everyone from the SAYN community to be able to contribute to the project. Please find below the guidelines of the SAYN development process.

## SAYN Development Process

* The first SAYN version was `0.1`.
* The master branch is the current development branch where pull requests are merged by default. **There are no other branches except for bug fixes of the latest version.**
* SAYN therefore uses the following branch structure:
    * master (last version + current development)
    * bug fix branches (to fix latest version bugs only)
* Every time a version is released, we tag the commit on master with the released version number.
* All changes of the WIP version are pushed to the master branch via pull requests.
* All bug fixes are pushed to master using a lower versioning level (e.g. `0.1.x`).

## How To Contribute

SAYN's contribution model is a standard fork model. Everyone, including the main contributors, work from forks of the main SAYN repository and send pull requests to it. Please see below the guidelines for the fork process:

1. Fork the SAYN repository.
2. [Configure the remote to point to the upstream repository](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/configuring-a-remote-for-a-fork) (i.e. SAYN's main repository).
3. [Sync your fork with the main repository to ensure it is up to date](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/syncing-a-fork).

Once you have the above in place, every contribution should be ticket based and use pull requests. The process is as follows:

1. Once you pick a ticket, open a pull request directly using the following naming convention: TBC.
2. In order to create the pull request, create a branch on your forked repository following this naming convention: TBC.
3. Push this new branch and set the remote as upstream.
4. [Create the pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) following the afore mentioned naming convention.
5. Make your development push to the pull request when finalised.
6. Request a review from one of the main contributors.
7. When reviewed and accepted, merge into master.
