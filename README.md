# FALocalRepo-Database

Database functionality for [falocalrepo](https://gitlab.com/MatteoCampinoti94/falocalrepo).

## Tables

To store its information, the database uses four tables: `SETTINGS`, `USERS`, `SUBMISSIONS` and `JOURNALS`.

### Settings

The settings table contains settings for the program and statistics of the database.

* `USRN` number of users in the `USERS` table
* `SUBN` number of submissions in the `SUBMISSIONS` table
* `HISTORY` list of executed commands in the format `[["<time1>", "<command1>"], ..., ["<timeN>", "<commandN>"]]` (UNIX time in seconds)
* `COOKIES` cookies for the scraper, stored in JSON format
* `FILESFOLDER` location of downloaded submission files
* `VERSION` database version, this can differ from the program version

### Users

The users table contains a list of all the users that have been download with the program, the folders that have been downloaded and the submissions found in each of those.

Each entry contains the following fields:

* `USERNAME` The URL username of the user (no underscores or spaces)
* `FOLDERS` the folders downloaded for that specific user.
* `GALLERY`
* `SCRAPS`
* `FAVORITES`
* `MENTIONS` this is a legacy entry used by the program up to version 2.11.2 (was named `EXTRAS`)
* `JOURNALS`

### Submissions

The submissions table contains the metadata of the submissions downloaded by the program and information on their files 

* `ID` the id of the submission
* `AUTHOR` the username of the author (uploader) in full format
* `TITLE`
* `UDATE` upload date in the format YYYY-MM-DD
* `DESCRIPTION` description in html format
* `TAGS` keywords sorted alphanumerically and comma-separated
* `CATEGORY`
* `SPECIES`
* `GENDER`
* `RATING`
* `FILELINK` the remote URL of the submission file
* `FILEEXT` the extensions of the downloaded file. Can be empty if the file contained errors and could not be recognised upon download
* `FILESAVED` 1 if the file was successfully downloaded and saved, 0 if there was an error during download

### Journals

The journals table contains the metadata of the journals downloaded by the program.

* `ID` the id of the journal
* `AUTHOR` the username of the author (uploader) in full format
* `TITLE`
* `UDATE` upload date in the format YYYY-MM-DD
* `CONTENT` content in html format

## Submission Files

The `save_submission` functions saves the submission metadata in the database and stores the files.

Submission files are saved in a tiered tree structure based on their submission ID. ID's are zero-padded to 10 digits and then broken up in 5 segments of 2 digits; each of this segments represents a folder tha will be created in the tree.

For example, a submission `1457893` will be padded to `0001457893` and divided into `00`, `01`, `45`, `78`, `93`. The submission file will then be saved as `00/01/45/78/93/submission.file` with the correct extension extracted from the file itself - FurAffinity links do not always contain the right extension and often confuse jpg and png.

## Upgrading Database

The `update_database` function allows to upgrade the database to the current version.

_Note:_ Versions before 2.7.0 are not supported by falocalrepo-database version 3.0.0 and above. To update from those to the new version use [falocalrepo](https://gitlab.com/MatteoCampinoti94/FALocalRepo/-/releases/v2.11.2) version 2.11.2 to update the database to version 2.7.0

### 2.7.x &rarr; 3.0.0

Information from the database are copied over to the new version, but otherwise remain unaltered save for a few changed column names in the `SUBMISSIONS` and `USERS` tables.

Files are moved to the new structure and the old files folder is deleted. Only submissions files are kept starting from version 3.0.0

### 3.0.x &rarr; 3.1.0

`EXTRAS` field in `USERS` table is changed to `MENTIONS`, and `extras` and `Extras` folders are renamed to `mentions` and `mentions_all` respectively.

### 3.1.x &rarr; 3.2.0

Add `JOURNALS` table and `JOURNALS` field in `USERS` table.

### 3.2.x &rarr; 3.3.0

Remove `LASTSTART` and `LASTUPDATE` entries and add `HISTORY` entry in `SETTINGS` table.

### 3.3.x, 3.4.x &rarr; 3.5.0

Changes in database functions, simply update `VERSION`.