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
    PREREQ_PM    => {
        'Moose' => 0,
        'MooseX::Types::Path::Class' => 0,

        'KiokuDB' => 0.07,

        'Data::Stream::Bulk' => 0,

        'BerkeleyDB::Manager' => '0.07',

        'Test::use::ok' => 0,
        'Test::More' => 0,
        'Test::TempDir' => 0,

        'Storable' => 0,

        'Search::GIN' => 0,
    },
);
