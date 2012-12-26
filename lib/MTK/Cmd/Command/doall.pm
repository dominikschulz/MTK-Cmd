package MTK::Cmd::Command::doall;
# ABSTRACT: execute a given SQL stmt on any db and/or table

use 5.010_000;
use mro 'c3';
use feature ':5.10';

use Moose;
use namespace::autoclean;

# use IO::Handle;
# use autodie;
# use MooseX::Params::Validate;
# use Carp;
use English qw( -no_match_vars );
# use Try::Tiny;
use File::Basename qw();

use MTK::DB;

# extends ...
extends 'MTK::Cmd::Command';

# has ...
has 'dbs' => (
    'is'            => 'ro',
    'isa'           => 'Str',
    'required'      => 0,
    'traits'        => [qw(Getopt)],
    'cmd_aliases'   => 'd',
    'documentation' => 'Process these databases',
);

has 'excludes' => (
    'is'            => 'ro',
    'isa'           => 'Str',
    'required'      => 0,
    'traits'        => [qw(Getopt)],
    'cmd_aliases'   => 'e',
    'documentation' => 'Excludes thoses patterns',
);

has 'tables' => (
    'is'            => 'ro',
    'isa'           => 'Str',
    'required'      => 0,
    'traits'        => [qw(Getopt)],
    'cmd_aliases'   => 't',
    'documentation' => 'Process all tables',
);

has 'user' => (
    'is'            => 'ro',
    'isa'           => 'Str',
    'required'      => 1,
    'traits'        => [qw(Getopt)],
    'cmd_aliases'   => 'u',
    'documentation' => 'Login',
);

has 'pass' => (
    'is'            => 'ro',
    'isa'           => 'Str',
    'required'      => 1,
    'traits'        => [qw(Getopt)],
    'cmd_aliases'   => 'p',
    'documentation' => 'Password',
);

has 'port' => (
    'is'            => 'ro',
    'isa'           => 'Int',
    'required'      => 0,
    'default'       => 3306,
    'traits'        => [qw(Getopt)],
    'cmd_aliases'   => 'o',
    'documentation' => 'MySQLd Port',
);

has 'socket' => (
    'is'            => 'ro',
    'isa'           => 'Str',
    'required'      => 0,
    'traits'        => [qw(Getopt)],
    'cmd_aliases'   => 's',
    'documentation' => 'MySQLd Socket',
);

has 'nobinlog' => (
    'is'            => 'ro',
    'isa'           => 'Bool',
    'required'      => 0,
    'default'       => 0,
    'traits'        => [qw(Getopt)],
    'cmd_aliases'   => 'b',
    'documentation' => 'Write command to binlog?',
);

has 'cmd' => (
    'is'            => 'ro',
    'isa'           => 'Str',
    'required'      => 0,
    'traits'        => [qw(Getopt)],
    'cmd_aliases'   => 'c',
    'documentation' => 'A SQL command',
);

has 'file' => (
    'is'            => 'ro',
    'isa'           => 'Str',
    'required'      => 0,
    'traits'        => [qw(Getopt)],
    'cmd_aliases'   => 'f',
    'documentation' => 'A SQL file',
);

has 'host' => (
    'is'            => 'ro',
    'isa'           => 'ArrayRef[Str]',
    'required'      => 1,
    'traits'        => [qw(Getopt)],
    'cmd_aliases'   => 'h',
    'documentation' => 'A list of database hosts to execute all commands on',
);

has '_queries' => (
    'is'      => 'rw',
    'isa'     => 'ArrayRef',
    'default' => sub { [] },
);

# with ...
# initializers ...

# your code here ...
sub _read_sql_file {
    my $self = shift;

    if ( open( my $SQL_FH, '<', $self->file() ) ) {
        my @lines = <$SQL_FH>;

        # DGR: we're just reading ...
        ## no critic (RequireCheckedClose)
        close($SQL_FH);
        ## use critic
        my $tmp_query;
        foreach my $line (@lines) {
            next if $line =~ m/^\s*#/;
            $line =~ s/\s*#.*$//;
            $line =~ s/^\s+//;
            $tmp_query .= $line;
            if ( $tmp_query && $tmp_query =~ m/;\s*$/ ) {
                push( @{ $self->_queries() }, $tmp_query );
            }
            if ( !$self->tables() && $line =~ m/_TABLENAME_/ ) {
                my $msg = 'Need to use --tables switch if you want to use _TABLENAME_ keyword in your queries!';
                $self->logger()->log( message => $msg, level => 'warning', );
                croak($msg);
            }
        } ## end foreach my $line (@lines)
        return 1;
    } ## end if ( open( my $SQL_FH,...))
    return;
} ## end sub _read_sql_file

sub execute {
    my $self = shift;

    if ( !$self->dbs() && !$self->tables() ) {
        my $msg = 'Specify either --tables or --dbs!';
        $self->logger()->log( message => $msg, level => 'warning', );
        croak($msg);
    }
    if ( !$self->cmd() && ( !$self->file() || !-r $self->file() ) ) {
        my $msg = 'Specify --file and point to a valid sql file or use --cmd to define an adhoc query!';
        $self->logger()->log( message => $msg, level => 'warning', );
        croak($msg);
    }

    if ( $self->file() ) {
        $self->_read_sql_file();
    }
    else {
        push( @{ $self->_queries() }, $self->cmd() );
    }

    # add localhost as default if no other host is given!
    if ( scalar( @{ $self->host() } ) < 1 ) {
        push( @{ $self->host() }, 'localhost' );
    }

  HOST: foreach my $host ( @{ $self->host() } ) {
        $self->_process_host($host);
    }    # end foreach

    return 1;
} ## end sub execute

sub _process_host {
    my $self = shift;
    my $host = shift;

    my $DB = MTK::DB::->new(
        {
            'username' => $self->user(),
            'password' => $self->pass(),
            'hostname' => $host,
            'socket'   => $self->socket(),
            'database' => 'mysql',
            'port'     => $self->port(),
            'logger'   => $self->logger(),
        }
    );

    if ( !$DB || !$DB->valid() ) {
        $self->logger()->log( message => 'Connection to '.$host.' failed.', level => 'error', );
        next HOST;
    }
    $self->logger()->log( message => 'Processing Host '.$host.' w/ DSN ' . $DB->dsn(), level => 'notice', );

    # OnlyDB        : STRING    : only return tables from this DB
    # ReturnHashRef : BOOL      : return a hash with extende information instead of the default array of `DB`.`TABLE`?
    # Excludes      : ARRAY     : filter out tables matching these patterns
    # IncludeOnly   : ARRRAY    : only return tables matching these patterns
    # AddHost       : STRING    : add a hostname in front of the table name
    # OnlyEngine    : STRING    : return only tables of this engine type (MyISAM, InnoDB, etc.)
    # NotOnlyBaseTables : BOOL  : return NOT only base tables (i.e. views too)
    my $msg_hdr = 'DB.TABLE - ENGINE - QUERY - STATUS - OUTPUT';
    $self->logger()->log( message => $msg_hdr, level => 'notice', );
    print $msg_hdr. "\n";
    my $table_ref = $DB->list_tables(
        {
            'ReturnHashRef' => 1,
            'Excludes'      => $self->excludes(),
            'Incldues'      => $self->dbs(),
        }
    );
    {

        # enable outflush so the query is printed before the result - so
        # see whats going on. normally its only flushed on each newline.
        ## no critic (ProhibitLocalVars)
        local $OUTPUT_AUTOFLUSH = 1;
        ## use critic
        foreach my $db ( keys %{$table_ref} ) {

            # DGR: 'coz of /i
            ## no critic (ProhibitFixedStringMatches)
            next if $db =~ m/^mysql$/i;
            ## use critic
            foreach my $table ( keys %{ $table_ref->{$db} } ) {
                my $engine = $table_ref->{$db}{$table}{'engine'};
                foreach my $qry (@{$self->queries()}) {
                    my $query = $qry;
                    $query =~ s/_DBNAME_/`$db`/g;
                    $query =~ s/_TABLENAME_/`$table`/g;
                    $DB->do('SET sql_log_bin = 0') if $self->nobinlog();
                    my $stm = $DB->prepare($query);

                    my $msg = "$db.$table - $engine - $query - ";
                    print $msg;

                    if ( $stm->execute() ) {
                        $msg .= 'OK';
                        $self->logger()->log( message => $msg, level => 'notice', );
                        print 'OK - ';
                    }
                    else {
                        $DB->do('SET sql_log_bin = 1') if $self->nobinlog();
                        $msg .= 'ERROR';

                        print "ERROR\n";
                        next;
                    } ## end else [ if ( $stm->execute() )]

                    $self->_print_results($stm);

                    # Done
                    $stm->finish();
                    $DB->do('SET sql_log_bin = 1') if $self->nobinlog();
                } ## end foreach my $qry (@queries)
            } ## end foreach my $table ( keys %{...})
        } ## end foreach my $db ( keys %{$table_ref...})
    }
    $DB->disconnect();

    $self->logger()->log( message => 'Finished Host '.$host, level => 'notice', );
    return 1;
} ## end sub _process_host

sub _print_results {
    my $self = shift;
    my $stm  = shift;

    # fetch in a sensible way, i.e. print column headers and a table
    # of results if there is more than one line/value.
    my $hash_ref = $stm->fetchrow_hashref();

    # the whole idea of this buffer code is to allow a distinction between
    # queries that return only on row and column (like select count(*) from table)
    # and queries that return more than that. these simple queries
    # will get their return value reported on line, the others get a table.
    my $buffer      = q{};
    my $first_col   = undef;
    my $one_row_col = 1;       # only one row in output?
    if ( scalar( keys( %{$hash_ref} ) ) > 1 ) {
        $one_row_col = 0;
    }
    $buffer .= "Result:\n";

    # Heading
    $buffer .= '| ';
    foreach my $key ( keys( %{$hash_ref} ) ) {
        $buffer .= $key.' | ';
    }
    $buffer .= "\n";

    # First row
    $buffer .= '| ';
    foreach my $key ( keys( %{$hash_ref} ) ) {
        if ( !$first_col ) {
            $first_col = $hash_ref->{$key};
        }
        $buffer .= $hash_ref->{$key} . ' | ';
    } ## end foreach my $key ( keys( %{$hash_ref...}))
    $buffer .= "\n";

    # Reamaining rows
    while ( my @row = $stm->fetchrow_array() ) {
        if ($one_row_col) {
            $one_row_col = 0;
            print $buffer;
        }
        print '| ';
        foreach my $row (@row) {
            print $row. ' | ';
        }
        print "\n";
    } ## end while ( my @row = $stm->fetchrow_array...)
    if ($one_row_col) {
        print 'Result: ' . $first_col . "\n";
    }
    return 1;
} ## end sub _print_results

sub abstract {
    return 'Execute a given bit of SQL on every table';
}

no Moose;
__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

MTK::Cmd::Command::doall - execute a given SQL stmt on any db and/or table

=method abstract

Workaround.

=method execute

Run the doall command.

=head1 ACKNOWLEDGEMENT

This module was originally developed for eGENTIC Systems. With approval from eGENTIC Systems,
this module was generalized and published, for which the authors would like to express their
gratitude.

=cut
