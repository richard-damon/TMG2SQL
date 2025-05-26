# MIT License
#
# Copyright (c) 2019 Richard Damon
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# External Dependency: dbfread
# Install with pip install dbfread
"""Make an SQLite file from a TMG database set"""

import argparse
import configparser
import datetime
import dbfread
from fnmatch import fnmatch
# import glob
import logging
import os
from pathlib import Path
from pprint import pformat
import sqlite3
import sys

LOG = logging.getLogger(__name__)
args = argparse.Namespace()     # Object with options,

Version = "0.1.2"


def _(x):
    """Hook to latter add language support"""
    return x


# Table Mapping Field Types to SQL Types
typemap = {
    'F': 'FLOAT',
    'L': 'BOOLEAN',
    'I': 'INTEGER',
    'C': 'TEXT',
    'N': 'REAL',  # because it can be integer or float
    'M': 'TEXT',
    'D': 'TEXT_DATE',
    'T': 'TEXT_DATETIME',
    '0': 'INTEGER',
}

# Common Links for info Table
TABLE_NAME = 'Name'             # Key for the name of the table (not actually used)
PRIMARY = 'Primary'             # Key to specify the Primary Key, Name of Column, or tuple of Columns
FOREIGN = 'Foreign'             # Key for dictionary of Foreign Keys. Key of dict is Column. (Will make the index)
INDEX = 'Index'                 # Key for set of Columns to Index
UNIQUE = 'Unique'               # Key for set of Columns to Define a Unique index
DATE = 'Date'                   # Marks fields with TMG Date (May want to create a translated version of the date)

PERSON = ('$', 'PER_NO')        # Link to a Person
DSID = ('D', 'DSID')            # Link to a Dataset
ETYPE = ('T', 'ETYPENUM')       # Link to Event Type
NAME = ('N', 'RECNO')           # Link to a Name

# Table Info for TMG Version 5-9 Tables
# Key is File Ending
#   Key:Name        A descritive name for the table
#   Key:Primary     The Primary Key (or Keys as a tuple) for the Table
#   Key:Foreign     Foreign Key References, Defined but no enforced
#       Key:        Column referencing other columns
#       Value:      Tuple of Table Key, Column Name
#   Key:Index       Index Definitions
#       Key:        Name For index
#       Value:      Column, or Tuple of Columns to build an index on
#
table_info = {
    # Data Set Tables
    'D':
        {
            TABLE_NAME: 'Data Sets',
            PRIMARY:    'DSID',
            # NAMESTYLE     -> ST.STYLEID       // Circular, perhaps add post import check
            # PLACESTYLE    -> ST.STYLEID       // Circular, perhaps add post import check / Add Foreign Key?
        },
    # Person Tables
    'C':        # Needs D
        {
            TABLE_NAME: 'Flags',
            PRIMARY:    'FLAGID',
            INDEX:      {
                'FLAGLABEL',        # Might be unique with DSID
                'FLAGFIELD',        # Might be unique with DSID
                'SEQUENCE',         # Might be unique with DSID
            },
            FOREIGN:    {
                'DSID':     DSID,
            },
        },
    '$':        # Needs D, to understand some need C
        {
            TABLE_NAME: 'Person',
            PRIMARY:    'PER_NO',
            UNIQUE:     'REF_ID',   # User defined ID Number
            DATE:       ('PBIRTH', 'PDEATH'),

            FOREIGN:    {
                'FATHER':   PERSON,
                'MOTHER':   PERSON,
                'DSID':     DSID,
                'SPOULAST': PERSON,
            },
            # Check FATHER is Primary Father
            # CHECK MOTHER is Primary Mother
            # SPOULAST is a spouse
            # PBIRTH is the Primary Birth Date
            # PDEATH is the Primary Death Date
        },
    # Focus Group Tables
    'O':        # Needs: none
        {
            TABLE_NAME: 'Focus Group',
            PRIMARY:    'GROUPNUM',
            INDEX:      {
                'GROUPNAME',
            },
        },
    'B':        # Needs D, $, O
        {
            TABLE_NAME: 'Focus Group Member',
            PRIMARY:    ('GROUPNUM', 'MEMBERNUM'),
            FOREIGN:    {
                'GROUPNUM': ('O', 'GROUPNUM'),  # Focus Group
                'MEMBERNUM':PERSON,
                'DSID':     DSID,
            },
        },

    # Mics People
    'DNA':      # Needs D, $
        {
            TABLE_NAME: 'DNA',
            PRIMARY:    'ID_DNA',
            FOREIGN:    {
                'DSID': DSID,
                'ID_PERSON': PERSON,
            },
        },

    'K':        # Needs D, $
        {
            TABLE_NAME: 'Timeline',
            PRIMARY:    ('TNAME', 'IDLOCK'),
            FOREIGN:    {
                'IDLOCK': PERSON,
                'DSID': DSID,
            }
        },
    'XD':       # Needs D, $
        {
            TABLE_NAME: 'Excluded Pair',
            PRIMARY:    ('PER1', 'PER2'),
            FOREIGN:    {
                'DSID':     DSID,
                'PER1':     PERSON,
                'PER2':     PERSON,
            },
        },

    # Styles
    'NPT':      # Needs D
        {
            TABLE_NAME: 'Name Part Type',
            PRIMARY:    'ID',
            UNIQUE:     'TEMPLATE',     # Might need DSID
            INDEX:      {
                'VALUE',
                'SHORTVALUE',
            },
            FOREIGN:    {
                'DSID': DSID,
            }
        },

    'ST':       # Needs: D
        {
            TABLE_NAME: 'Style',
            PRIMARY:    'STYLEID',
            INDEX:      'STYLENAME',
            FOREIGN:    {
                'DSID':     DSID,
            },
        },
    # Perhaps a second pass through D to add default styles?

    # Places
    'P':        # Needs D, ST
        {
            TABLE_NAME: 'Place',
            PRIMARY:    'RECNO',
            FOREIGN:    {
                'STYLEID': ('ST', 'STYLEID'),
                'DSID': DSID,
            },
        },

    'PD':       # Needs: none
        {
            TABLE_NAME: 'Place Dictionary',
            PRIMARY:    'UID',
            INDEX:      {
                'VALUE',
                'SDX',
            },
        },

    'PPT':      # Needs: D
        {
            TABLE_NAME: 'Place Part Type',
            PRIMARY:    'ID',
            INDEX:  {
                'VALUE',
                'SHORTVALUE',
            },
            FOREIGN:    {
                'DSID': DSID,
            }
        },

    'PPV':      # Needs D, P, PD, PPT
        {
            TABLE_NAME: 'Place Part Value',
            PRIMARY:    ('RECNO', 'TYPE'),
            FOREIGN:    {
                'RECNO':    ('P', 'RECNO'),
                'UID':      ('PD', 'UID'),
                'ID':       ('PPT', 'ID'),
                'DSID':     DSID,
            },
        },

    # Tags

    'T':        # Needs D
        {
            TABLE_NAME: 'Tag Type',
            PRIMARY:    'ETYPENUM',
            INDEX:      {
                'ETYPENAME',
                'GEDCOM_TAG',
            },
            FOREIGN:    {
                'DSID': DSID,
            },
        },

    'F':        # Needs D, $, T
        {
            TABLE_NAME: 'Relationship',
            PRIMARY:    'RECNO',
            FOREIGN: {
                'CHILD': PERSON,
                'PARENT': PERSON,
                'DSID': DSID,
                'PTYPE': ETYPE,         # ETYPE -> ADMIN should be 2, 3, or 12 (non-primary only)
            },
            # TEST_UNIQUE (CHILD, PARENT)
            # TEST_PRIMARY CHILD matches FATHER/MOTHER (based on ETYPE->ADMIN being 2 or 3
        },

    'ND':       # Needs none
        {
            TABLE_NAME: 'Name Dictionary',
            PRIMARY:    'UID',
            INDEX:  {
                'VALUE',
                'SDX',
            },
        },

    'N':        # Needs D, $, ST, T, ND
        {
            TABLE_NAME: 'Name',
            PRIMARY:    'RECNO',
            INDEX:      'SRTDATE',
            DATE:       ('NDATE', 'SRTDATE'),
            FOREIGN: {
                'NPER':     PERSON,
                'ALTYPE':   ETYPE,
                'DSID':     DSID,
                'STYLEID':  ('ST', 'STYLEID'),
                'SURID':    ('ND', 'UID'),
                'GIVID':    ('ND', 'UID'),
            },
            # CHECK Unique NPER for PRIMARY names, and matches Person Primary Name
        },

    'NPV':      # Needs N, ND
        {
            TABLE_NAME: 'Name Part Value',
            PRIMARY:    ('RECNO', 'TYPE'),
            FOREIGN:    {
                'RECNO':    ('N', 'RECNO'),
                'UID':      ('ND', 'UID'),
                'ID':       ('NPT', 'ID'),
                'DSID':     DSID,
            }
        },

    'G':
        {
            TABLE_NAME: 'Event',
            PRIMARY:    'RECNO',
            DATE:       ('EDATE', 'SRTDATE'),
            FOREIGN:    {
                'ETYPE':    ETYPE,
                'DSID':     DSID,
                'PER1':     PERSON,
                'PER2':     PERSON,
            },
        },

    'E':
        {
            TABLE_NAME: 'Event Witness',
            PRIMARY:    ('GNUM', 'EPER'),
            FOREIGN:    {
                'EPER': PERSON,
                'GNUM': ('G', 'RECNO'),
                'DSID': DSID,
                'NAMEREC': NAME,
            },
            # Check PRIMARY, only PER1 or PER2, only one of that type, check PBIRTH, PDEATH
        },

    # Source
    'A':
        {
            # PRIMARY 'RULESET', 'SOURTYPE'
            TABLE_NAME: 'Source Type',
            PRIMARY:    ('RULESET', 'SOURTYPE'),
            INDEX:      'NAME',
            FOREIGN:    {
                'DSID': DSID,
                'TRANS_TO': ( 'A', {'RULESET': '1', 'SOURTYPE': None}),
                'SAMEAS':   ( 'A', {'RULESET': '1', 'SOURTYPE': None}),
            },
            # RULESET: 1, 2. 3
            # ONLY ONE PRIMARY (per DATASET?)
        },

    'U':
        {
            TABLE_NAME: 'Source Element',
            PRIMARY:    'RECNO',
            UNIQUE:     'ELEMENT',
            INDEX:      {
                'GROUPNUM',     # 1-32
            },
            FOREIGN:    {
                'DSID': DSID,
            },
        },

    'M':
        {
            TABLE_NAME: 'Source',
            PRIMARY:    'MAJNUM',
            INDEX:      {
                'REF_ID',
                'ABBREV',
                'TITLE',
            },
            FOREIGN:    {
                'SPERNO':       PERSON,
                'SUBJECTID':    PERSON,
                'COMPILERID':   PERSON,
                'EDITORID':     PERSON,
                'SPERNO2':      PERSON,
                'DSID':         DSID,
                'TYPE':         ('A', {'RULESET': 1, 'SOURCETYPE': None }),
                'CUSTTYPE':     ('A', {'RULESET': 3, 'SOURCETYPE': None }),
            }
        },

    'R':
        {
            TABLE_NAME: 'Repository',
            PRIMARY: 'RECNO',
            INDEX:   {
                'NAME',
                'ABBREV',
            },
            FOREIGN: {
                'DSID':     DSID,
                'RPERNO':   PERSON,
                'ADDRESS':  ('P', 'RECNO')
            },
        },

    'W':
        {
            TABLE_NAME: 'Repository Link',
            PRIMARY: ('MNUMBER', 'RNUMBER'),
            FOREIGN: {
                'MNUMBER': ('M', 'MAJNUM'),
                'RNUMBER': ('R', 'RECNO'),
                'DSID': DSID,
            },
            # Check PRIMARY is unique
        },

    'S':
        {
            TABLE_NAME: 'Citation',
            PRIMARY:    'RECNO',
            FOREIGN:    {
                'MAJSOURCE': ('M', 'MAJNUM'),
                'DSID': DSID,
                # REFREC, N/F/M/G/P/S=STYPE, RECNO
            },
        },

    # Misc

    'L':
        {   # No Primary as can have duplicate records on a given item.
            TABLE_NAME: 'Research Log',
            FOREIGN: {
                'RLPER1':   PERSON,
                'RLPER2':   PERSON,
                'RLGTYPE':  ETYPE,
                'DSID':     DSID,
                'ID_PERSON':PERSON,
                'ID_EVENT': ('G', 'RECNO'),
                'ID_SOURCE':('M', 'MAJNUM'),
                'ID_REPOS': ('R', 'RECNO'),
                # RLNUM Links based on RLTYPE
            }
        },

    'I':
        {
            TABLE_NAME: 'Exhibit',
            PRIMARY:  'IDEXHIBIT',
            INDEX:      {
                'XNAME',
            },
            FOREIGN: {
                'RLPER1':   PERSON,
                'RLPER2':   PERSON,
                'RLGTYPE':  ETYPE,
                'DSID':     DSID,
                'ID_PERSON':PERSON,
                'ID_EVENT': ('G', 'RECNO'),
                'ID_SOURCE':('M', 'MAJNUM'),
                'ID_REPOS': ('R', 'RECNO'),
                'ID_CIT':   ('S', 'RECNO'),
                'ID_PLACE': ('P', 'RECNO'),
            },
            # Check RLTYPE points to the type that has the link, are RLNUM is that number.
            # Check RLPER1, RLPER2 are the principals of ID_EVENT
        },

    'PICK1':        # NOT DOCUMENTED

        {
            TABLE_NAME: 'Pick List',
            FOREIGN:    {
                'REF_ID':   ('$', 'REF_ID'),
                'FATHER':   ('$', 'REF_ID'),
                'MOTHER':   ('$', 'REF_ID'),
            },
        },
    }

table_map = {}


def do_sql(cursor, statement, parms=None):
    """Execute an SQL command"""
    if parms is None:
        # DDL statements
        LOG.info(statement)
        cursor.execute(statement)
    else:
        # DML Statements
        LOG.debug(pformat(parms))
        cursor.execute(statement, parms)


def show(*words):
    """Print a line of test from parameters"""
    LOG.info('  ' + ' '.join(str(word) for word in words))


def show_field(field):
    """Display information about a Database Field"""
    LOG.warning('    {} ({} {})'.format(field.name, field.type, field.length))


def show_table(dbf):
    """Show Information from DBF file"""
    show('Name:', dbf.name)
    show('Memo File:', dbf.memofilename or '')
    show('DB Version:', dbf.dbversion)
    show('Records:', len(dbf))
    show('Deleted Records:', len(dbf.deleted))
    show('Last Updated:', dbf.date)
    show('Character Encoding:', dbf.encoding)
    show('Fields:')
    for field in dbf.fields:
        show_field(field)


def copy_dbf(filename, tbl: str, conn, info=None):
    """Copy a DBF file into the Database"""
    if info is None:
        info = {}
    print(filename, '', end='')
    LOG.warning(f"\n{filename}")
    LOG.info(pformat(info))
    dbf = dbfread.DBF(filename)
    cursor = conn.cursor()
    tablename = dbf.name    # Name the tables in the SQL after the name of the DBF
    table_map[tbl] = tablename

    if args.verbose > 0:
        show_table(dbf)
    do_sql(cursor, 'drop table if exists %s' % tablename)
    field_types = {}
    for field in dbf.fields:
        field_types[field.name] = typemap.get(field.type, 'TEXT')  
    #
    # Create the table
    #

    table_prop = ""

    pkey = info.get(PRIMARY, None)
    if isinstance(pkey, str):
        field_types[pkey] += ' PRIMARY KEY'    # Add PRIMARY KEY to the Primary Key
    elif isinstance(pkey, tuple):
        table_prop += ",\n    PRIMARY KEY (" + (', '.join(pkey)) + ")"
    elif pkey is not None:
        LOG.error(f'TODO Primary: {pkey}')

    unique = info.get(UNIQUE, None)
    if isinstance(unique, str):
        # Single Unique field in the table
        field_types[unique] += " UNIQUE"
    elif isinstance(unique, set):
        # A Tuple of Unique Fields in the table
        for col in unique:
            if isinstance(col, str):
                field_types[col] += " UNIQUE"
            elif isinstance(col, tuple):
                table_prop += ',\n   UNIQUE(' + (', '.join(table_prop)) + ')'
            else:
                LOG.error(f'TODO Unique {unique} : {col}')
    elif unique is not None:
        LOG.error(f'TODO Unique: {unique}')

    fkeys = info.get(FOREIGN, None)
    if fkeys is not None:
        index = info.get(INDEX, None)
        # Make index entry if needed
        if index is None:
            info[INDEX] = set()
        elif isinstance(index, str):
            # Convert single entry index to a set
            info[INDEX] = {index}

        for key, value in fkeys.items():
            if isinstance(key, str):
                # Single Key Foreign Constraint
                if isinstance(value[1], str):
                    # Single to Dictionary with constant Foreign Keys do not translate
                    info[INDEX].add(key)
                    field_types[key] += f' REFERENCES "{table_map[value[0]]}"("{value[1]}")'
            else:
                # When we handle multi-key foreign keys, need to also change to Copy loop
                LOG.error(f'TODO Foreign {key}: {value}')

    col_defs = ',\n    '.join(['"%s" %s' % (f, field_types[f])
                               for f in dbf.field_names])

    sql = f'CREATE TABLE "{tablename}" (\n    {col_defs}{table_prop}\n)'

    do_sql(cursor, sql)

    # Add any requested Indexes
    index = info.get(INDEX, None)
    if index is not None:
        if isinstance(index, str):
            # Single index specified
            index_name = tablename + '_' + index
            sql = f'DROP INDEX IF EXIST "{index_name}"'
            do_sql(cursor, sql)
            sql = f'CREATE INDEX "{index_name}" ON "{tablename}"("{index}")'
            do_sql(cursor, sql)
        elif isinstance(index, set):
            # tuple of indexes specific
            for col in index:
                if isinstance(col, str):
                    # Single index specified
                    index_name = tablename + '_' + col
                    sql = f'DROP INDEX IF EXISTS "{index_name}"'
                    do_sql(cursor, sql)
                    sql = f'CREATE INDEX "{index_name}" ON "{tablename}"("{col}")'
                    do_sql(cursor, sql)
                elif isinstance(col, tuple):
                    index_name = tablename + '_' + ('_'.join(col))
                    sql = f'DROP INDEX IF EXISTS "{index_name}"'
                    do_sql(cursor, sql)
                    sql = f'CREATE INDEX "{index_name}" ON "{tablename}"(' + (', '.join(col)) + ')'
                    do_sql(cursor, sql)
                else:
                    LOG.error(f'TODO Index: {index} : {col}')
        else:
            LOG.error(f'TODO Index: {index}')

    LOG.info(pformat(info))

    # Create data rows
    refs = ', '.join([':' + f for f in dbf.field_names])
    sql = 'insert into "%s" values (%s)' % (tablename, refs)
    LOG.debug(sql)
    recno = 0
    progress = 1000
    for rec in dbf:
        # Convert Foreign Keys 0 to NULL
        if fkeys is not None:
            # LOG.info(pformat(rec))
            for col, reference in fkeys.items():
                if isinstance(col, str):
                    if rec[col] == 0:
                        rec[col] = None
        for col in rec.keys():
            if isinstance(rec[col], datetime.date):
                rec[col] = rec[col].strftime('%Y-%m-%d')
            elif isinstance(rec[col], datetime.datetime):
                rec[col] = rec[col].strftime('%Y-%m-%d %H:%M:%S')


        LOG.debug(rec)
        # TODO Should add validation of Foreign Keys (and maybe add a more advanced validata operation.
        try:
            cursor.execute(sql, rec)
        except sqlite3.Error as err:
            print('')
            print("Error: ", err, "Rec= ", rec, "\n", sql)
            LOG.error(f"{err}: {rec}")

        recno += 1
        if (recno % progress) == 0:
            num = recno // progress
            if num % 10 == 0:
                print('', num, '', end='')
            else:
                print(num % 10, end='')
    conn.commit()
    print('')  # Add a return

    if fkeys is not None:
        # Check if any references are broken
        todo = False
        ckey = {}
        sql = f'SELECT * from {tablename}'
        res = cursor.execute(sql)
        for row in res:
            for key, value in fkeys.items():
                if row[key] is not None:
                    ref_table = table_map[value[0]]
                    ref_col = value[1]
                    if isinstance(ref_col, str):
                        parms = {'Value': row[key]}
                        sql = f'SELECT "{ref_col}" FROM "{ref_table}" WHERE "{ref_col}" == :Value'
                        res = cursor.execute(sql, parms)
                        res = res.fetchall()
                        if len(res) < 1:
                            print(f'Missing Reference: {key} in \n{pformat(dict(row))}')
                            LOG.error(f'Missing Reference: {key} in \n{pformat(dict(row))}\n{sql}')
                    else:
                        todo = True
                        ckey[key] = ( ref_table, ref_col)
        if todo:
            LOG.error(f'TODO Complex Foreign Keys: \n{pformat(ckey)}')

def tmg2db(projname, conn):
    """ Convert a TMG Project to a SQL Database

    Parameters:
    projname -- path/name of the TMG .pjc file
    conn -- Database connection to use to write the database
    """

    table_map.clear()

    config = configparser.ConfigParser()
    LOG.warning(config.read(projname))
    tablename = projname.stem + 'pjc'
    cursor = conn.cursor()
    conn.row_factory = sqlite3.Row
    do_sql(cursor, 'DROP TABLE IF EXISTS %s' % tablename)
    sql = '''CREATE TABLE "%s" ("section" 'TEXT', "key" 'TEXT', "value" 'TEXT')''' % (tablename,)
    do_sql(cursor, sql)
    sql = '''INSERT INTO "%s" VALUES (:section, :key, :value)''' % (tablename,)
# TODO Read the pjc file and put it into a table in the database of Group / key / value    
    for section in config.sections():
        for key in config[section]:
            do_sql(cursor, sql, {"section": section, "key": key, "value": str(config[section][key])})
    conn.commit()

    length = len(projname.stem)-1
    path = projname.parent
    base = projname.stem[:-1]
    pat = (base + '*.dbf').upper()
    for tbl in table_info.keys():
        file = path.joinpath(base + tbl + ".dbf")
        if file.exists():
            copy_dbf(file, tbl, conn, table_info[tbl])
        else:
            print("Missing:", file)
            LOG.error("Missing File {file}")

    # Process any unknown file type
    for fname in os.listdir(projname.parent):
        if fnmatch(fname.upper(), pat):
            dbf = path.joinpath(fname)
            tbl = dbf.stem[length:].upper()
            info = table_info.get(tbl, None)
            if info is None:
                print("Unknown: fname")
                LOG.warning(f"\n{tbl}  {info}")
                copy_dbf(path.joinpath(fname), tbl, conn, info)


def tmg2sqlite(projname):
    """Convert a TMG Project to Sqlite.

    Parameters:
    projname -- path to the TMG project .pjc file

    Creates a Sqlite database by the same name as the project with a '.Sqlite' extension
    Tables within the database have names matching the names of the .dbf files
    uses: https://github.com/olemb/dbfread/
    """
    print(projname)
    path = Path(projname)
    path.resolve()
    if not path.exists():
        print("File "+projname+" Doesn't Exist")
        return
    sdb = path.with_suffix('.sqlite')
    # Allow options of other types of output
    # TMG Seems to only use N fields for integers, and Sqlite will still store floats as floats
    typemap["N"] = "INTEGER"
    conn = sqlite3.connect(str(sdb))
    cursor = conn.cursor()
    do_sql(cursor, 'PRAGMA foreign_keys = OFF')     # While we are processing ignore Foreign Key Errors
    tmg2db(path, conn)


def find_file(path: Path, pat: str):
    """Search Path for all files that match pat and then process"""
    print("Processing:", path, pat)
    for filename in os.listdir(path):
        fullname = (path / filename).resolve()
        if filename[0] == '.':
            # Ignore files and directories beginning with .
            pass
        elif os.path.isdir(fullname):
            print("\nDir: ", filename)
            LOG.warning(f"Dir {filename}")
            if args.recursive:
                find_file(fullname, pat)
        elif fnmatch(filename.upper(), pat):
            print('File: ', filename)
            LOG.warning(f"\nFile {filename}")
            tmg2sqlite(path.joinpath(filename))


# Main Function
# Provides the implementation of the package as a utility
#
# Usage;
# TMG2SQL project.pjc
#
# Converts the specified project (wild cards allowed) to an Sqlite database
# with the same root name as the project.
#
# TODO: could use a lot of work to make  a more general utility
def main():
    """Main Function"""

    # Output File & Type
    print("TMG2Sqlite Version: ", Version)

    # TODO add argument processing
    parser = argparse.ArgumentParser(description='TMG2SQL')
    parser.add_argument('file', nargs='*', help=_('Files to convert'), default="*.pjc")
    parser.add_argument('-d', '--dir', help=_('Directory to process'), default=".")
    parser.add_argument('-l', '--log', action='store_true', help=_('Use Log File'))
    parser.add_argument('-r', '--recursive', action='store_true', help=_('Recursively process Directories'))
    parser.add_argument('-v', '--verbose', action="count", help=_('Verbose Mode'))

    parser.parse_args(namespace=args)
    if not isinstance(args.file, list):
        args.file = [args.file]

    # Resolve the starting Directory
    base_dir = Path(args.dir).resolve()
    # process file patterns one at a time:

    if args.log:
        log_file = Path(base_dir, "TMG2SQL.log")
        try:
            os.remove(log_file)
        except FileNotFoundError:
            pass
        fh = logging.FileHandler(log_file)
        # message_format = config['Log'].get('msg.' + cmd.lower(), FILE_FORMAT)
        # ff = logging.Formatter(message_format)
        # fh.setFormatter(ff)
        LOG.addHandler(fh)

    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    max_verb = len(levels) - 1
    if args.verbose is None:
        args.verbose = 0
    if args.verbose > max_verb:
        args.verbose = max_verb

    LOG.setLevel(levels[args.verbose])

    for filename in args.file:
        filename = Path(base_dir / filename).resolve()
        path = filename.parent
        pat = filename.name.upper()
        find_file(path, pat)


if __name__ == "__main__":
    if 'idlelib.run' in sys.modules:
        sys.argv.extend(('../Family/*.pjc',))   # Default Arguments to use in IDLE
    main()
