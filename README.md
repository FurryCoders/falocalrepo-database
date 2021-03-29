# FALocalRepo-Database

[![version_pypi](https://img.shields.io/pypi/v/falocalrepo-database?logo=pypi)](https://pypi.org/project/falocalrepo-database/)
[![version_gitlab](https://img.shields.io/badge/dynamic/json?logo=gitlab&color=orange&label=gitlab&query=%24%5B%3A1%5D.name&url=https%3A%2F%2Fgitlab.com%2Fapi%2Fv4%2Fprojects%2Fmatteocampinoti94%252Ffalocalrepo-database%2Frepository%2Ftags)](https://gitlab.com/MatteoCampinoti94/falocalrepo-database)
[![version_python](https://img.shields.io/pypi/pyversions/falocalrepo-database?logo=Python)](https://www.python.org)

Database functionality for [falocalrepo](https://gitlab.com/MatteoCampinoti94/falocalrepo).

## Usage

_Detailed usage and components documentation will be added in a future patch_

## Tables

To store its information, the database uses four tables: `SETTINGS`, `USERS`, `SUBMISSIONS` and `JOURNALS`.

**Note**: bar-separated lists are formatted as `|item1||item2|` to properly isolate all elements

### Settings

The settings table contains settings for the program and statistics of the database.

* `HISTORY` list of executed commands in the format `[[<time1>, "<command1>"], ..., [<timeN>, "<commandN>"]]` (UNIX time
  in seconds)
* `COOKIES` cookies for the scraper, stored in JSON format
* `FILESFOLDER` location of downloaded submission files
* `VERSION` database version

### Users

The users table contains a list of all the users that have been download with the program, the folders that have been
downloaded, and the submissions found in each of those.

Each entry contains the following fields:

* `USERNAME` The URL username of the user (no underscores or spaces)
* `FOLDERS` the folders downloaded for that specific user, sorted and bar-separated

### Submissions

The submissions table contains the metadata of the submissions downloaded by the program and information on their files

* `ID` the id of the submission
* `AUTHOR` the username of the author (uploader) in full format
* `TITLE`
* `DATE` upload date in the format YYYY-MM-DD
* `DESCRIPTION` description in html format
* `TAGS` keywords sorted alphanumerically and bar-separated
* `CATEGORY`
* `SPECIES`
* `GENDER`
* `RATING`
* `TYPE` image, text, music, or flash
* `FILEURL` the remote URL of the submission file
* `FILEEXT` the extensions of the downloaded file. Can be empty if the file contained errors and could not be recognised
  upon download
* `FILESAVED` file and thumbnail download status: 00, 01, 10, 11. 1*x* if the file was downloaded 0*x* if not, *x*1 if
  thumbnail was downloaded, *x*0 if not
* `FAVORITE` a bar-separated list of users that have "faved" the submission
* `MENTIONS` a bar-separated list of users that are mentioned in the submission description as links
* `FOLDER` the folder of the submission (`gallery` or `scraps`)
* `USERUPDATE` whether the submission was added as a user update or favorite/single entry

### Journals

The journals table contains the metadata of the journals downloaded by the program.

* `ID` the id of the journal
* `AUTHOR` the username of the author (uploader) in full format
* `TITLE`
* `DATE` upload date in the format YYYY-MM-DD
* `CONTENT` content in html format
* `MENTIONS` a bar-separated list of users that are mentioned in the journal content as links
* `USERUPDATE` whether the journal was added as a user update or single entry

## Submission Files

The `save_submission` functions saves the submission metadata in the database and stores the files.

Submission files are saved in a tiered tree structure based on their submission ID. ID's are zero-padded to 10 digits
and then broken up in 5 segments of 2 digits; each of this segments represents a folder tha will be created in the tree.

For example, a submission `1457893` will be padded to `0001457893` and divided into `00`, `01`, `45`, `78`, `93`. The
submission file will then be saved as `00/01/45/78/93/submission.file` with the correct extension extracted from the
file itself - FurAffinity links do not always contain the right extension and often confuse jpg and png.

## Upgrading Database

The `FADatabase.upgrade` function allows to upgrade the database to the current version.

_Note:_ Versions before 2.7.0 are not supported by falocalrepo-database version 3.0.0 and above. To update from those to
the new version use [falocalrepo](https://gitlab.com/MatteoCampinoti94/FALocalRepo/-/releases/v2.11.2) version 2.11.2 to
update the database to version 2.7.0
