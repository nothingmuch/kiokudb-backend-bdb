#!/usr/bin/perl -w

use strict;

use ExtUtils::MakeMaker;

require 5.008; # utf8 and what not

WriteMakefile(
    NAME         => 'KiokuDB::Backend::BDB',
    VERSION_FROM => 'lib/KiokuDB/Backend/BDB.pm',
    INSTALLDIRS  => 'site',
    SIGN         => 1,
    PL_FILES     => { },
    META_MERGE   => { configure_requires => { BerkeleyDB => '0.36' } },
    PREREQ_PM    => {
        'Moose' => 0,
        'MooseX::Types::Path::Class' => 0,

        'KiokuDB' => 0.08,

        'Data::Stream::Bulk' => 0,

        'BerkeleyDB::Manager' => '0.09',
        'BerkeleyDB' => '0.36',

        'Test::use::ok' => 0,
        'Test::More' => 0,
        'Test::TempDir' => 0,

        'Storable' => 0,

        'Search::GIN' => 0,
    },
);

if ( eval { require BerkeleDB } ) {
    die "BDB 4.7 required, but we only have $BerkeleyDB::db_version" if $BerkeleyDB::db_version < 4.7;
}
