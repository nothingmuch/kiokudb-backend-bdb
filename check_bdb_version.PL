#!/usr/bin/perl

use strict;
use warnings;

use BerkeleyDB;

warn "BerkeleyDB version is $BerkeleyDB::VERSION, library $BerkeleyDB::db_version\n";

die "OS unsupported\nYour version of the Berkeley DB library (not the module) is too old ($BerkeleyDB::db_version, we need 4.7)\n" unless $BerkeleyDB::db_version >= 4.7;

