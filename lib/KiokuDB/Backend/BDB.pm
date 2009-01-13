#!/usr/bin/perl

package KiokuDB::Backend::BDB;
use Moose;

use Carp qw(croak);

use Scalar::Util qw(weaken);
use MooseX::Types::Path::Class qw(Dir);

use BerkeleyDB qw(DB_NOOVERWRITE DB_KEYEXIST);

use KiokuDB::Backend::BDB::Manager;

use namespace::clean -except => 'meta';

# TODO use a secondary DB to keep track of the root set
# integrate with the Search::GIN bdb backend for additional secondary indexing

# this will require storing GIN extracted data in the database, too

# also port Search::GIN's Data::Stream::Bulk/BDB cursor code
# this should be generic (work with both c_get and c_pget, and the various
# flags)

our $VERSION = "0.11";

with qw(
    KiokuDB::Backend
    KiokuDB::Backend::Serialize::Delegate
    KiokuDB::Backend::Role::Clear
    KiokuDB::Backend::Role::TXN
    KiokuDB::Backend::Role::TXN::Nested
    KiokuDB::Backend::Role::Scan
    KiokuDB::Backend::Role::Query::Simple::Linear
);

has manager => (
    isa => "KiokuDB::Backend::BDB::Manager",
    is  => "ro",
    coerce => 1,
    required => 1,
    #handles => "KiokuDB::Backend::TXN",
);

sub new_from_dsn_params {
    my ( $self, %args ) = @_;

    my %manager = %args;

    if ( my $dir = delete $args{dir} ) {
        $manager{home} = $dir;
    }

    $self->new(manager => \%manager, %args);
}

sub txn_begin { shift->manager->txn_begin(@_) }
sub txn_commit { shift->manager->txn_commit(@_) }
sub txn_rollback { shift->manager->txn_rollback(@_) }
sub txn_do { shift->manager->txn_do(@_) }

has primary_db => (
    is      => 'ro',
    isa     => 'Object',
    lazy_build => 1,
);

sub BUILD { shift->primary_db } # early

sub _build_primary_db {
    my $self = shift;

    $self->manager->open_db("objects", class => "BerkeleyDB::Hash");
}

sub delete {
    my ( $self, @ids_or_entries ) = @_;

    my @uids = map { ref($_) ? $_->id : $_ } @ids_or_entries;

    my $primary_db = $self->primary_db;
    foreach my $id ( @uids ) {
        if ( my $ret = $primary_db->db_del($id) ) {
            die $ret;
        }
    }
}

sub insert {
    my ( $self, @entries ) = @_;

    my $primary_db = $self->primary_db;

    foreach my $entry ( @entries ) {
        my $ret = $primary_db->db_put(
            $entry->id => $self->serialize($entry),
            ( $entry->has_prev ? () : DB_NOOVERWRITE ),
        );

        if ( $ret ) {
            if ( $ret == DB_KEYEXIST ) {
                croak "Entry " . $entry->id . " already exists in the database";
            } else {
                die $ret;
            }
        }
    }
}

sub get {
    my ( $self, @uids ) = @_;

    my ( $var, @ret );

    my $primary_db = $self->primary_db;

    foreach my $uid ( @uids ) {
        $primary_db->db_get($uid, $var) == 0 || return;
        push @ret, $var;
    }

    return map { $self->deserialize($_) } @ret;
}

sub exists {
    my ( $self, @uids ) = @_;
    my $primary_db = $self->primary_db;
    my $v;
    map { $primary_db->db_get($_, $v) == 0 } @uids;
}

sub clear {
    my $self = shift;

    my $count = 0;

    $self->primary_db->truncate($count);

    return $count;
}

sub all_entries {
    my $self = shift;

    $self->manager->cursor_stream(
        db => $self->primary_db,
        values => 1,
    )->filter(sub {[ map { $self->deserialize($_) } @$_ ]});
}

sub all_entry_ids {
    my $self = shift;

    $self->manager->cursor_stream(
        db => $self->primary_db,
        keys => 1,
    );
}

# sub root_entries { } # secondary index?

__PACKAGE__->meta->make_immutable;

__PACKAGE__

__END__

=pod

=head1 NAME

KiokuDB::Backend::BDB - L<BerkeleyDB> backend for L<KiokuDB>.

=head1 SYNOPSIS

    KiokuDB->connect( "bdb:dir=/path/to/storage", create => 1 );

=head1 DESCRIPTION

This is a L<BerkeleyDB> based backend for L<KiokuDB>.

It is the best performing backend for most tasks, and is very feature complete
as well.

The L<KiokuDB::Backend::BDB::GIN> subclass provides searching support using
L<Search::GIN>.

=head1 ATTRIBUTES

=over 4

=item manager

The L<BerkeleyDB::Manager> instance that opens up the L<BerkeleyDB> databases.

This will be coerced from a hash reference too, so you can do something like:

    KiokuDB::Backend::BDB->new(
        manager => {
            home => "/path/to/storage",
            create => 1,
            transactions => 0,
        },
    );

to control the various parameters.

=back

=head1 VERSION CONTROL

L<http://github.com/nothingmuch/kiokudb-backend-bdb>

=head1 AUTHOR

Yuval Kogman E<lt>nothingmuch@woobling.orgE<gt>

=head1 COPYRIGHT

    Copyright (c) 2008, 2009 Yuval Kogman, Infinity Interactive. All
    rights reserved This program is free software; you can redistribute
    it and/or modify it under the same terms as Perl itself.

=cut

