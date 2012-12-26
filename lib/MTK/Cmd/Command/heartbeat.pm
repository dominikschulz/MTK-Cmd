package MTK::Cmd::Command::heartbeat;
# ABSTRACT: write a timestamp to a db, every second

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
use Linux::Pidfile;

use MTK::DB;
use MTK::DB::Credentials;

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
        'pidfile' => $self->config()->get( 'MTK::Heartbeat::Pidfile', { Default => '/var/run/mtk-heartbeat.pid', } ),
    });

    return $Pidfile;
}

# your code here ...
sub execute {
    my $self = shift;

    $self->_pidfile()->create() or die('Script already running.');

    my $hostname = $self->config()->get('MTK::Heartbeat::DB::Hostname')  || 'localhost';
    my $port     = $self->config()->get('MTK::Heartbeat::DB::Port')      || 3306;
    my $db       = $self->config()->get('MTK::Heartbeat::DB::DBName')    || 'REPLICHECK';
    my $table    = $self->config()->get('MTK::Heartbeat::DB::TableName') || 'heartbeat';

    my $Creds = MTK::DB::Credentials::->new(
        {
            'config'   => $self->config(),
            'hostname' => $hostname,
            'keys'     => [qw(MTK::Heartbeat::DB)],
            'logger'   => $self->logger(),
        }
    );
    my $DB  = $Creds->dbh();
    my $dsn = $DB->dsn();
    $Creds = undef;

    if ( !$DB || !$DB->valid() ) {
        $self->logger()->log( message => "Database not available. DSN: $dsn, Error: " . DBI->errstr, level => 'error', );
        return;
    }

    my $query = 0;    # Hold query strings
    my $prepq = 0;    # Hold prepared query

    $query = 'UPDATE ' . $table . ' SET ts=? WHERE id=0';
    $self->logger()->log( message => 'Query: '.$query, level => 'debug', );
    $prepq = $DB->prepare($query);

    my $running = 1;
    while ($running) {
        my $current_time = time();
        $self->logger()->log( message => 'Updating to current time: '.$current_time, level => 'debug', );

        # use an alarm timer to prevent hanging
        # reconnect if connection dies
        my $timeout      = 30;
        my $prev_timeout = 0;
        my $ok           = eval {
            local $SIG{ALRM} = sub { die "alarm-mtk-hearbeat\n"; };
            $prev_timeout = alarm $timeout;
            if ( !$prepq->execute($current_time) ) {

                # reconnect if necessary
                $DB->check_connection();
            }

            # restore previous alarm, if any
            alarm $prev_timeout;
            1;
        };

        # make sure the alarm is off
        alarm $prev_timeout;
        if ( $EVAL_ERROR && $EVAL_ERROR eq "alarm-mtk-hearbeat\n" ) {
            $self->logger()->log( message => 'Connection timed out after '.$timeout, level => 'warning', );
            sleep 30;
        }
        elsif ( !$ok ) {
            $self->logger()->log( message => 'Eval failed somehow (unknown error).', level => 'warning', );
        }
        sleep(1);
    }
    $prepq->finish();
    $DB->disconnect();
    return 1;
}

sub abstract {
    return 'Constantly write a timestamp to a given MySQL table. Used as a heartbeat for mtk delay.';
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

MTK::Cmd::Command::heartbeat - write a timestamp to a db, every second

=method DEMOLISH

Remove our pidfile.

=method abstract

Workaround.

=method execute

run the heartbeat command.

=head1 ACKNOWLEDGEMENT

This module was originally developed for eGENTIC Systems. With approval from eGENTIC Systems,
this module was generalized and published, for which the authors would like to express their
gratitude.

=cut
