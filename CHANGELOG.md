# Changelog

## 3.0.0
  * Complete rewrite using streams-based architecture [#29](https://github.com/singer-io/tap-quickbase/pull/29)
  * Upgrade to Python 3.12
  * Update dependencies: singer-python 6.3.0, backoff 2.2.1
  * Add unit and integration tests

## 2.0.3
  * Adds a proper circleci config; makes pylint happy; bump library versions [#27](https://github.com/singer-io/tap-quickbase/pull/27)

## 2.0.2
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 2.0.1
  * Detect out of range timestamps before emitting records and provide context to help identify the faulty record [#19](https://github.com/singer-io/tap-quickbase/pull/19)

## 2.0.0
  * Replace spaces and hyphens in field names with underscores, remove all other non-alphanumeric characters

## 1.0.3
  * Ensures that the base url has a trailing slash and uses the HTTPS protocol.

## 1.0.2
  * Use metadata in all situations instead of custom schema extensions. [#13](https://github.com/singer-io/tap-quickbase/pull/13)

## 1.0.1
  * Now throws an informative error when the app being connected contains no tables.

## 1.0.0
  * Initial release.
