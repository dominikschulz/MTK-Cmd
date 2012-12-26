package MTK::Cmd::Command::delay;
# ABSTRACT: keep a mysql slave a given number of seconds behind the master

use 5.010_000;
use mro 'c3';
use feature ':5.10';

use Moose;
use namespace::autoclean;

# use IO::Handle;
# use autodie;
# use MooseX::Params::Validate;
# use Carp;
# use English qw( -no_match_vars );
# use Try::Tiny;
use MTK::DB;
use MTK::DB::Credentials;
use Linux::Pidfile;

# extends ...
extends 'MTK::Cmd::Command';
# has ...
has '_pidfile' => (
    'is'    => 'ro',
    'isa'   => 'Linux::Pidfile',
    'lazy'  => 1,
    'builder' => '_init_pidfile',
);
# with ...
# initializers ...
sub _init_pidfile {
    my $self = shift;

    my $Pidfile = Linux::Pidfile::->new({
        'logger' => $self->logger(),
        'pidfile' => $self->config()->get( 'MTK::Delay::Pidfile', { Default => '/var/run/mtk-delay.pid', } ),
    });

    return $Pidfile;
}

# your code here ...
sub execute {
    my $self = shift;

    $self->_pidfile()->create() or die('Script already running.');

    my $running = 1;

    my $host     = $self->config()->get('MTK::Delay::DB::Host') || 'localhost';
    my $port     = $self->config()->get('MTK::Delay::DB::Port') || 3306;
    my $database = $self->config()->get('MTK::Delay::DB::DBName')    or croak('Need MTK::Delay::DB::DBName');
    my $table    = $self->config()->get('MTK::Delay::DB::TableName') or croak('Need MTK::Delay::DB::TableName');
    my $Creds    = MTK::DB::Credentials::->new(
        {
            'config'   => $self->config(),
            'hostname' => $host,
            'keys'     => [qw(MTK::Delay::DB)],
            'logger'   => $self->logger(),
        }
    );
    my $username = $Creds->username();
    my $password = $Creds->password();
    my $DB       = $Creds->dbh();
    $Creds = undef;

    if ( !$DB ) {
        $self->logger()->log( message => 'Database not available. User: '.$username.', Host: '.$host.', Port: '.$port.', Error: ' . DBI->errstr, level => 'emerg', );
        return;
    }

    my $query = 'SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?';
    my $sth   = $DB->prepexec( $query, $database, $table );
    my $count = $sth->fetchrow_array($sth);
    $sth->finish();
    my $sss;
    if ( $count > 0 ) {
        $query = 'SELECT ts FROM `'.$database.'`.`'.$table.'` WHERE id=0 LIMIT 1';
        $sss   = $DB->prepare($query);
    }

    my $holdbacktime = $self->config()->get('MTK::Delay::Holdbacktime');
    my $checktime    = $self->config()->get('MTK::Delay::Checktime');

    while ($running) {

        # reconnect if necessary

        if ( !$DB->check_connection() ) {
            $self->logger()->log( message => 'Could not connect to DSN: ' . $DB->dsn() . ' - ' . $DB->errstr(), level => 'alert', );
        }

        # Record the slave status
        my $secbehind = -1;
        if ($sss) {
            $sss->execute();
            $secbehind = $sss->fetchrow_array();
        }
        else {
            $secbehind = $DB->get_sec_behind_master();
        }

        $self->logger()->log( message => "Seconds_behind_Master: $secbehind", level => 'debug', );

        if ( $secbehind < $holdbacktime ) {
            $DB->lock_tables( { 'heartbeat' => 'WRITE', } );
            $self->logger()->log( message => "Locking $table: $query", level => 'debug', );
        }
        else {
            $DB->unlock_tables();
            $self->logger()->log( message => "Unlocking $table", level => 'debug', );
        }
        sleep($checktime);
    }
    $sss->finish() if $sss;
    $DB->disconnect();
    return 1;
}

sub abstract {
    return 'Keep an MySQL Slave a given number of seconds behind the master';
}

sub DEMOLISH {
    my $self = shift;

    $self->_pidfile()->remove();

    return 1;
}

no Moose;
__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

MTK::Cmd::Command::delay - keep a mysql slave a given number of seconds behind the master

=method DEMOLISH

Remove our pidfile.

=method abstract

Workaround.

=method execute

Run the delay command.

=head1 ACKNOWLEDGEMENT

This module was originally developed for eGENTIC Systems. With approval from eGENTIC Systems,
this module was generalized and published, for which the authors would like to express their
gratitude.

=cut
