<div align="center">

<img alt="logo" width="400" src="https://raw.githubusercontent.com/FurryCoders/Logos/main/logos/falocalrepo-database-transparent.png">

# FALocalRepo-Database

Database functionality for [falocalrepo](https://pypi.org/project/falocalrepo).

[![](https://img.shields.io/github/v/tag/FurryCoders/falocalrepo-database?label=github&sort=date&logo=github&color=blue)](https://github.com/FurryCoders/falocalrepo-database)
[![](https://img.shields.io/pypi/v/falocalrepo-database?logo=pypi)](https://pypi.org/project/falocalrepo-database/)
[![](https://img.shields.io/pypi/pyversions/falocalrepo-database?logo=Python)](https://www.python.org)

</div>

## Tables

To store its information, the database uses separate tables: `USERS`, `SUBMISSIONS`, `JOURNALS`, `SETTINGS`,
and `HISTORY`.

**Note**: bar-separated lists are formatted as `|item1||item2|` to properly isolate all elements

### Users

The users' table contains a list of all the users that have been download with the program, the folders that have been
downloaded, and the submissions found in each of those.

Each entry contains the following fields:

* `USERNAME` the URL username of the user (no underscores or spaces)
* `FOLDERS` the folders downloaded for that specific user, sorted and bar-separated
* `ACTIVE` `1` if the user is active, `0` if not
* `USERPAGE` the user's profile text

### Submissions

The submissions' table contains the metadata of the submissions downloaded by the program and information on their files

* `ID` the id of the submission
* `AUTHOR` the username of the author (uploader) in full format
* `TITLE`
* `DATE` upload date in ISO format _YYYY-MM-DDTHH:MM_
* `DESCRIPTION` description in html format
* `TAGS` bar-separated tags
* `CATEGORY`
* `SPECIES`
* `GENDER`
* `RATING`
* `TYPE` image, text, music, or flash
* `FILEURL` the remote URL of the submission file
* `FILEEXT` the extensions of the downloaded file. Can be empty if the file contained errors and could not be recognised
  upon download
* `FILESAVED` file and thumbnail download status as a 2bit flag: `1x` if the file was downloaded `0x` if not, `x1` if
  thumbnail was downloaded, `x0` if not. Possible values are `0`, `1`, `2`, `3`.
* `FAVORITE` a bar-separated list of users that have "faved" the submission
* `MENTIONS` a bar-separated list of users that are mentioned in the submission description as links
* `FOLDER` the folder of the submission (`gallery` or `scraps`)
* `USERUPDATE` `1` if the submission was added as a user update otherwise `0`

### Journals

The journals' table contains the metadata of the journals downloaded by the program.

* `ID` the id of the journal
* `AUTHOR` the username of the author (uploader) in full format
* `TITLE`
* `DATE` upload date in ISO format _YYYY-MM-DDTHH:MM_
* `CONTENT` content in html format
* `MENTIONS` a bar-separated list of users that are mentioned in the journal content as links
* `USERUPDATE` `1` if the journal was added as a user update otherwise `0`

### Comments

The comments' table contains the metadata of the journals and submissions stored in the database.

* `ID` the id of the comment
* `PARENT_TABLE` `SUBMISSIONS` if the comment relates to a submission, `JOURNAL` if the comment relates to a journal
* `PARENT_ID` the id of the parent object (submission or journal)
* `REPLY_TO` the id of the parent comment, if the comment is a reply
* `AUTHOR` the username of the author in full format
* `DATE` post date in ISO format _YYYY-MM-DDTHH:MM:SS_
* `TEXT` the text of the comment

### Settings

The settings table contains settings for the program and variable used by the database handler and main program.

* `COOKIES` cookies for the download program, stored in JSON format
* `FILESFOLDER` location of downloaded submission files
* `VERSION` database version

### History

The history table holds events related to the database.

* `TIME` event time in ISO format _YYYY-MM-DDTHH:MM:SS.ssssss_
* `EVENT` the event description

## Submission Files

The `save_submission` functions saves the submission metadata in the database and stores the files.

Submission files are saved in a tiered tree structure based on their submission ID. IDs are zero-padded to 10 digits and
then broken up in 5 segments of 2 digits; each of these segments represents a folder tha will be created in the tree.

For example, a submission `1457893` will be padded to `0001457893` and divided into `00`, `01`, `45`, `78`, `93`. The
submission file will then be saved as `00/01/45/78/93/submission.file` with the correct extension extracted from the
file itself (FurAffinity links do not always contain the right extension and sometimes confuse JPEG and PNG).

## Upgrading Database

_Note:_ versions prior to 4.19.0 are not supported by falocalrepo-database version 5.0.0 and above. To update from
those, use [falocalrepo v3.25.0](https://pypi.org/project/falocalrepo/v3.25.0) to upgrade the database to version
4.19.0.<br/>
_Note:_ Versions prior to 2.7.0 are not supported by falocalrepo-database version 3.0.0 and above. To update from those
to the new version use [falocalrepo v2.11.2](https://github.com/FurryCoders/FALocalRepo/releases/tag/v2.11.2) to upgrade
the database to version 2.7.0
