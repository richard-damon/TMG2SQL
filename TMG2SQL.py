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

import configparser
import dbfread
import fnmatch
import glob
import os
import pathlib
import sqlite3
import sys

Version = "0.1.0"
# logfile = open('test.log', 'w')

# Table Mapping Field Types to SQL Types
typemap = {
    'F': 'FLOAT',
    'L': 'BOOLEAN',
    'I': 'INTEGER',
    'C': 'TEXT',
    'N': 'REAL',  # because it can be integer or float
    'M': 'TEXT',
    'D': 'DATE',
    'T': 'DATETIME',
    '0': 'INTEGER',
}

# Table Info for TMG Version 5-9 Tables
table_info = {
    '$':
        {
            'Name': 'Person',
            'Primary': 'PER_NO',        
        },
    'A':
        {
            'Name': 'Source Type',
        },
    'B':
        {
            'Name': 'Focus Group Member',
        },
    'C':
        {
            'Name': 'Flags',
            'Primary': 'FLAGID',
        },
    'D':
        {
            'Name': 'Data Sets',
            'Primary': 'DSID',  
        },
    'DNA':
        {
            'Name': 'DNA',
            'Primary': 'ID_DNA',
        },
    'E':
        {
            'Name': 'Event Witness',
        },
    'F':
        {
            'Name' : 'Relationship',
            'Primary':  'RECNO',
        },
    'G':
        {
            'Name': 'Event',
            'Primary':  'RECNO',
        },
    'I':
        {
            'Name': 'Exhibit',
            'Primary':  'IDEXHIBIT',
        },
    'K':
        {
            'Name': 'Timeline',
        },
    'L':
        {
            'Name': 'Research Log',
        },
    'M':
        {
            'Name': 'Source',
            'Primary': 'MAJNUM',
        },
    'N':
        {
            'Name': 'Name',
            'Primary':  'RECNO',
        },
    'ND':
        {
            'Name': 'Name Dictionary',
        },
    'NPT':
        {
            'Name': 'Name Part Type',
        },
    'NPV':
        {
            'Name': 'Name Part Value',
        },
    'O':
        {
            'Name':     'Focus Group',
            'Primary':  'GROUPNUM',
        },
    'P':
        {
            'Name':     'Place',
            'Primary':  'RECNO',
        },
    'PD':
        {
            'Name': 'Place Dictionary',
        },
    'PICK1':
        {
            'Name': 'Pick List'
        },
    'PPT':
        {
            'Name': 'Place Part Type',
        },
    'PPV':
        {
            'Name': 'Place Part Value',
        },
    'R':
        {
            'Name':     'Repository',
            'Primary':  'RECNO',
        },
    'S':
        {
            'Name':     'Citation',
            'Primary':  'RECNO',
       },
    'ST':
        {
            'Name':     'Style',
            'Primary':  'STYLEID',
        },
    'T':
        {
            'Name':     'Tag Type',
            'Primary':  'ETYPENUM',
        },
    'U':
        {
            'Name':     'Source Element',
            'Primary':  'RECNO',
        },
    'W':
        {
            'Name': 'Repository Link',
        },
    'XD':
        {
            'Name': 'Excluded Pair',
        },
    }

def doSQL(cursor, statement, parms = None):
    if(parms == None):
#        print(statement, file = logfile)
        cursor.execute(statement)
    else:
        cursor.execute(statement, parms)

def show(*words):
    print('  ' + ' '.join(str(word) for word in words), file=logfile)

def show_field(field):
    print('    {} ({} {})'.format(field.name, field.type, field.length), file=logfile)

def show_table(dbf):
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
    

def CopyDbf(filename, conn, info = None):
    if info is None: info = {}
    print(filename, end='')
    dbf = dbfread.DBF(filename)
    cursor = conn.cursor()
    tablename = dbf.name    # Name the tables in the SQL after the name of the DBF
    if(False): show_table(dbf)
    doSQL(cursor, 'drop table if exists %s' % tablename)
    field_types = {}
    for field in dbf.fields:
        field_types[field.name] = typemap.get(field.type, 'TEXT')  
    #
    # Create the table
    #
    if 'Primary' in info:
        key = info['Primary']
        field_types[key] = field_types[key] + ' PRIMARY KEY'    # Add PRIMARY KEY to the Primary Key
    
    defs = ', '.join(['"%s" %s' % (f, field_types[f])
                      for f in dbf.field_names])
    sql = 'create table "%s" (%s)' % (tablename, defs)
    doSQL(cursor, sql)
    # Create data rows
    refs = ', '.join([':' + f for f in dbf.field_names])
    sql = 'insert into "%s" values (%s)' % (tablename, refs)
#    print(sql, file=logfile)
    recno = 0
    progress = 1000
    for rec in dbf:
#        print(list(rec.values()), file=logfile)
        try:
            cursor.execute(sql, list(rec.values()))
        except sqlite3.Error as err:
            print('');
            print("Error: ", err, rec)
#            print(err, rec.values(), file=logfile)
        recno += 1
        if((recno % progress) == 0): print('', recno//progress, end='')
    conn.commit()
    print('') # Add a return


def TMG2DB(projname, conn):
    """ Convert a TMG Project to a SQL Database

    Parameters:
    projname -- path/name of the TMG .pjc file
    conn -- Database connection to use to write the database
    """
    config = configparser.ConfigParser()
#    print(config.read(projname), file=logfile)
    tablename = projname.stem + 'pjc'
    cursor = conn.cursor()
    doSQL(cursor, 'DROP TABLE IF EXISTS %s' % tablename)
    sql = '''CREATE TABLE "%s" ("section" 'TEXT', "key" 'TEXT', "value" 'TEXT')''' % (tablename,)
    doSQL(cursor, sql)
    sql = '''INSERT INTO "%s" VALUES (:section, :key, :value)''' % (tablename,)
# TODO Read the pjc file and put it into a table in the database of Group / key / value    
    for section in config.sections():
        for key in config[section]:
           doSQL(cursor, sql, {"section": section, "key":key, "value":str(config[section][key])})
    conn.commit();
    pat = str(projname.stem[:-1] + '*.dbf').upper()
    length = len(projname.stem)-1;
    path = projname.parent
    for fname in os.listdir(projname.parent):
        if(fnmatch.fnmatch(fname.upper(), pat)):
            dbf = path.joinpath(fname)
            type = dbf.stem[length:].upper()
            info = table_info.get(type, None)
#            print(type, ' ', info)
            CopyDbf(path.joinpath(fname), conn, info)

#
def TMG2Sqlite( projname ):
    """Convert a TMG Project to Sqlite.

    Parameters:
    projname -- path to the TMG project .pjc file

    Creates a Sqlite database by the same name as the project with a .Sqlite extension
    Tables within the database have names matching the names of the .dbf fileshttps://github.com/olemb/dbfread/
    """
    print(projname)
    path = pathlib.Path(projname)
    path.resolve()
    if(not path.exists()):
        print("File "+projname+" Doesn't Exist")
        return
    sdb = path.with_suffix('.sqlite');
    # Allow options of other types of output
    typemap["N"] = "INTEGER"        # TMG Seems to only use N fields for integers, and Sqlite will still store flaats as floats
    conn = sqlite3.connect(str(sdb))
    TMG2DB(path, conn)

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
    # TODO add argument processing
    # Output File & Type
    print("TMG2Sqlite Version: ", Version)
    print(sys.argv)
    name = "*.pjc" if(len(sys.argv) < 2) else sys.argv[1]
    path = pathlib.Path(name)
    direct = path.parent
    pat = path.name.upper()
    for fname in os.listdir(direct):
        if(fnmatch.fnmatch(fname.upper(), pat)):
           print(fname)
           TMG2Sqlite(direct.joinpath(fname)) 

if __name__ == "__main__":
    if 'idlelib.run' in sys.modules:
        sys.argv.extend(('../Family/*.pjc',))   # Default Arguments to use in IDLE
    main()

