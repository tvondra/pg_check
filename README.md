pg_check
========
An extension that provides basic consistency checking functionality for
tables and b-tree indexes. Currently this performs basic checks at the
page and item level, for example:

* page header features (lower <= upper <= special etc.)
* items not overlapping
* attributes not overflowing the tuple end
* invalid varlena sizes (negative or over 1GB, ...)

Moreover it's possible to cross-check the table and indexes, i.e. to
check if there are any missing / superfluous items in the index
(compared to the heap).

This extension **does not** implement correcting any of the issues,
nor it fully checks the index structure (except for the generic page
checks mentioned above). The extension does not support index types
other than b-tree (yet).


Installation
------------

This is a regular extension (9.1) or a contrib module (9.0), so it may be
installed rather easily - do either this

    $ make install
    $ psql dbname -c "CREATE EXTENSION pg_check"

or this (on 9.0)

    $ make install
    $ psql dbname < `pg_config --sharedir`/contrib/pg_check--0.1.0.sql

and the extension should be installed.


Functions
---------

Currently there are four functions available

 * `pg_check_table(name, blk_from, blk_to)` - checks range of blocks of
    the heap table
 * `pg_check_table(name, checkIndexes, crossCheck)` - checks the table
    with the options to check all indexes on it, and even cross-checking
    the indexes with the table
 * `pg_check_index(name, blk_from, blk_to)` - checks range of blocks for
    the index
 * `pg_check_index(name)` - checks a single index

So if you want to check table "my_table" and all the indexes on it, do this:

    db=# SELECT pg_check_table('my_table', true, true);

and it will print out info about the checks (and return number of issues).


Messages
--------

The functions may print various info about the blocks/tuples, depending on
the client_min_messages level.

* `WARNING` - only info about actual issues
* `DEBUG1` - info about pages
* `DEBUG2` - info about tuples on a page
* `DEBUG3` - info about attributes of a tuple


License
-------

This software is provided under the BSD license. See LICENSE for details.