# ProxyFS Security

We take the security of this project seriously. Like any complex
system, security must be vigilantly pursued. We need your help.

## How to report security issues

If you believe you've identified a vulnerability, please work with the
project maintainers to fix and disclose the issue responsibly. Email
security@swiftstack.com and include the following details in your
report:

* Description of the location and potential impact of the vulnerability
* Description of the steps required to reproduce the vulnerability
  (POC scripts, screenshots, and compressed screen captures are
  all helpful to us)

We will monitor this email address and promptly respond to any
vulnerabilities reported.

## How to propose and review a security patch

Note: The patch development and review process for security issues is
different than normal patches in ProxyFS. Because the GitHub issue
process is public, all security bugs must have patches proposed to and
reviewed via the security email address above.

After a patch for the reported bug has been developed locally, you the
patch author need to share that with the community. This is a simple
process, but it is different than the normal ProxyFS workflow.

* Export it using the `format-patch` command:

    ```
    git format-patch --stdout HEAD~1 >path/to/local/file.patch
    ```

  Now you have the patch saved locally and you can attach it to an email.

* For reviewers, to review the attached patch, run the following command:

    ```
    git am <path/to/local/file.patch
    ```

  This applies the patch locally as a commit, including the commit
  message and all other metadata. However, if the patch author did not
  use `format-patch` to export the patch (perhaps they used
  `git show >local.patch` ), then the patch can be applied locally with:

    ```
      git apply path/to/local/file.patch
    ```
