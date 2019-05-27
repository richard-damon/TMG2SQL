# TMG2SQL
A Utility to Convert a The Master Genealogist Database into a SQL/Sqlite Database 

This is a small python utility to convert a TMG Database into a Sqlite database,
and eventually 

## Distribution
This repository contains several forms of distribution of this module.

### Source / Root directory
In the root directory of this repository are the source file(s) that make up This
module. I have built it with Python 3.7.3, not sure what version of python is 
will work on.

There is also a build.bat to build the standalone executables (below) which
don't require the user of the program to have python (or the needed dependencies
installed). Using the build.bat batch file requires pyinstaller to be installed.

### Single File Executable /dist directory
There is a simple executable file, TMG2SQL.exe in the /dist directory of the 
repository. This file was created with pyinstall, and is a self contained Executable
with all the information to run the program. When run it will extract files into
a directory in the system temp folder so that it can run. Just place is somewhere
on your path, and you can use the utility. (I tend to have a /bin directory to place
such programs).

### Single Directory Distribution /dist/TMG2SQL directory
This is another version of the module built with pyinstall, but rather than needing
to expand to a temporary directory, it has all those files already present, so it
is quicker to run. Place the directory somewhere, and add an entry on your path to 
it (or put the directory somewhere on the path and run with TMG2SQL/TMG2SQL)

## Usage
### Program
**TMG2SQL [foo.pjc]**
This command will convert the foo.pjc TMG project into a Sqlite database by the 
same name. If no file is specified, then all TMG projects (or things it sees as
TMG projects) will be transfered into Sqlite databases.

The program should not disturb the TMG database, but please have a good backup first.

While running, it will print on the console the name of the project/file being
processed, and a number for every 1000 records that have been processed.

If an error occurs while writing the data (possibly due to corruption in the database 
making the Primary Keys not Unique) an error message will be printed including the 
field data that flagged the error.

### Python Module
This Module has several functions that can be used to help move TMG databases 
(or other dbf files) into Sqlite databases (or other SQL databases)

#### main
The Command Line Utility entry point. Parses the parameters and use the following
functions to do the work. 

#### TMG2Sqlite(projname)
Takes the TMG project specified by *projname* (which should point to the xxx.pjc file)
into a Sqlite database by the same name, but a .Sqlite extension. Later I hope to
add the ability to point this to a .SQZ file

####  TMG2DB(projname, conn)
Takes the TMG project specified by *projname* and copies it into the database 
specified by the database connection *conn*

In addition to copying the .dbf will also include information from the .pjc file
as a set of triples of section-name/item-name/value

Used by *TMG2Sqlite* to do the main work.

#### CopyDBF(filename, conn, info)
Copies the .dbf file specified by *filename* into the database specified by the 
connection *conn*. *info* provides some additional information about the table, 
currently what field (if any) is to be used as the Primary Key for the table.

The table name will be the base filename. (TODO: add an optional parameter to 
override this)

Used by *TMG2DB* for each table in the TMG Project, but can also be used for other
usages
 
## Dependancies:
+ Python: Developed in Python 3.7, not sure how old of a version of python it
will run in.

+ dbfred
 see https://github.com/olemb/dbfread/
 can be installed with: pip3 install dbfread
 
+ pyinstaller (to build the stand alone exe versions)
  see https://www.pyinstaller.org
  can be installed with pip3 install pyinstaller
 
## Future Work:
+ Allow specifying a .SQZ file, and if so extract it to a temp directory, and 
copy that to the Sqlite database next to the .SQZ file.
+ Add indexes (other than the current PRIMARY KEYS specified)
+ Add real argument parsing to enable adding the rest of the features
+ Ability to specify the output file
+ Ability to put multiple databases in one directory into a single output file
+ Ability to dump the SQL to create the Database
+ Ability to use other type of similar SQL Databases
+ CopyDBF: add parameter to specify table name
+ Maybe add the ability to specify a .dbf file to convert by itself
+ When used as a sub module, allow the creation of the tables as TEMPORARY, so
the calling application can use the database to extract the information into its
own format and then get rid of these tables.

## References
Wholly Genes has published a document describing the internal structure of the 
database files. The forum post for this is at:
http://www.whollygenes.com/forums201/index.php?/topic/381-file-structures-for-the-master-genealogist-tmg/

The file itself is at:
http://www.whollygenes.com/files/tmg9fstr.zip