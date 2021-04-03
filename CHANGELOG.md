# Changelog

## 4.12.0

* Add method to check database version against the library's with optional exception raised on error
* Add method to check if a database has multiple connections with optional exception raised on error

## 4.11.0

* `FILESAVED` stores information regarding both submission files and thumbnails
* Migration to Python 3.9

## 4.10.0

* Overhaul update functions with wrappers
* Improve 4.8 to 4.9 update
* Rename `FADatabase.update` to `merge`

## 4.9.0

* Add `TYPE` to `SUBMISSIONS`
* Submissions file are now saved as `submission` instead of `submission.` if the extension is blank

## 4.8.0

* Rename `FILELINK` to `FILEURL`
* Use `|` to separate lists fields to better isolate all items

## 4.7.0

* Added `MENTIONS` to `JOURNALS` 

## 4.6.0

* Add `USERUPDATE` column to `SUBMISSIONS` and `JOURNALS` to track if an item was added as a user update or not

## 4.5.0

* Added `MENTIONS` and `FOLDER` to `SUBMISSIONS`. Removed `GALLERY`, `SCRAPS`, `FAVORITES`, `MENTIONS` from `USERS` table.

## 4.4.0

* Added `FAVORITE` column to `SUBMISSIONS` to hold users that have "faved" the submission 

## 4.3.0

* Removed `JRNN`, `SUBN`, and `USRN` counters from `SETTINGS`.

## 4.2.0

* Changes in database functions, simply update `VERSION`.

## 4.0.0

* Rename `UDATE` column in `SUBMISSIONS` and `JOURNALS` to `DATE`. Add automatic insertion checks to all tables.

## 3.8.0

* Changes in database functions, simply update `VERSION`.

## 3.5.0

* Update `HISTORY` entry in `SETTINGS` to use the `List[List[float, str]]` format.

## 3.4.0

* Changes in database functions, simply update `VERSION`.

## 3.3.0

* Remove `LASTSTART` and `LASTUPDATE` entries and add `HISTORY` entry in `SETTINGS` table.

## 3.2.0

* Add `JOURNALS` table and `JOURNALS` field in `USERS` table.

## 3.1.0

* `EXTRAS` field in `USERS` table is changed to `MENTIONS`, and `extras` and `Extras` folders are renamed to `mentions` and `mentions_all` respectively.

## 3.0.0

* Information from the database are copied over to the new version, but otherwise remain unaltered save for a few changed column names in the `SUBMISSIONS` and `USERS` tables
  * Files are moved to the new structure, and the old files' folder is deleted. Only submissions files are kept starting from version 3.0.0.