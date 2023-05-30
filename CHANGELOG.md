# Changelog

## 2.0.3
  * Dependabot update [#26](https://github.com/singer-io/tap-quickbase/pull/26)

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