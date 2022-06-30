# Changelog

## 5.3.6

* `Database.merge` and `Database.copy` skip existing entries directly when replace is set to `False`

## 5.3.5

* Improve detection of plain text file

## 5.3.4

* Fix file folder error for 5.3.2 (now 5.3.4) update

## 5.3.3 YANKED

* Fix backup error

## 5.3.2 YANKED

* Fix broken extensions

## 5.3.1

* Fix extension error when saving submissions with special extensions

## 5.3.0

* Change `SUBMISSIONS.FILEURL` and `SUBMISSIONS.FILEEXT` to lists and support multiple files per submission
* Add backup method to `Database`

## 5.2.2

* Change keys of `COMMENTS` table to `ID`, `PARENT_TABLE`, `PARENT_ID`

## 5.2.0

* Add `COMMENTS` table

## 5.1.0

* Add boolean USERS.ACTIVE column to store whether user is active or not

## 5.0.0

* Complete rewrite with typing-powered formatting and parsing of row values

## 4.19.1

* `FADatabase.move_files_folder()` accepts extra argument to skip moving old files to new location

## 4.19.0

* Improved handling of `Selector` objects
* Create `Selector` objects more easily via the `SelectorBuilder` object

## 4.18.0

* `DATE` columns are now saved in ISO format `YYYY-MM-DDTHH:MM`

## 4.17.0

* Use new query syntax for selection based on MongoDB queries
* Add custom exceptions for version mismatch and multiple connections errors

## 4.16.0

* Use `pathlib` for all path operations
* Add `FADatabase.move_files_folder` to move files folder
* `FADatabaseSubmissions.get_submissions_files` returns `Path` objects

## 4.15.0

* Add `FADatabaseSubmissions.get_submissions_files` to retrieve submissions files and thumbnails
* Add `FADatabase.files_folder` to compute files folder path

## 4.14.0

* Change behaviour of `FADatabase.merge` to accept cursors like `FADatabase.copy`

## 4.13.0

* Use custom class to handle cursors and `SELECT` results
* Add copy method to database class to copy cursors to a second database

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

* Added `MENTIONS` and `FOLDER` to `SUBMISSIONS`. Removed `GALLERY`, `SCRAPS`, `FAVORITES`, `MENTIONS` from `USERS`
  table.

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

* `EXTRAS` field in `USERS` table is changed to `MENTIONS`, and `extras` and `Extras` folders are renamed to `mentions`
  and `mentions_all` respectively.

## 3.0.0

* Information from the database are copied over to the new version, but otherwise remain unaltered save for a few
  changed column names in the `SUBMISSIONS` and `USERS` tables
    * Files are moved to the new structure, and the old files' folder is deleted. Only submissions files are kept
      starting from version 3.0.0.