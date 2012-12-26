package MTK::Cmd::Command;
# ABSTRACT: baseclass for any MTK CLI command

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
use Config::Yak;
use Log::Tree;

# extends ...
extends 'MooseX::App::Cmd::Command';
# has ...
has '_config' => (
    'is'    => 'rw',
    'isa'   => 'Config::Yak',
    'lazy'  => 1,
    'builder' => '_init_config',
    'accessor' => 'config',
);

has '_logger' => (
    'is'    => 'rw',
    'isa'   => 'Log::Tree',
    'lazy'  => 1,
    'builder' => '_init_logger',
    'accessor' => 'logger',
);
# with ...
# initializers ...
sub _init_config {
    my $self = shift;

    my $Config = Config::Yak::->new({
        'locations'     => $self->_config_locations(),
    });

    return $Config;
}

sub _init_logger {
    my $self = shift;

    my $Logger = Log::Tree::->new($self->_log_facility());

    return $Logger;
}

# your code here ...
sub _config_locations {
    return [qw(conf /etc/mtk)];
}

sub _log_facility {
    return 'mtk-cli';
}

sub ask_yesno {
    my $self = shift;
    my $msg = shift;
    print $msg. ' [y/N]: ';
    ## no critic (ProhibitExplicitStdin)
    my $resp = <STDIN>;
    ## use critic
    chomp($resp);
    if ( $resp =~ m/(1|Yes|Ja|Y)/i ) {
        return 1;
    }
    return;
}

sub ask_yesnoall {
    my $self = shift;
    my $msg = shift;
    print $msg. ' [y/N/a]: ';
    ## no critic (ProhibitExplicitStdin)
    my $resp = <STDIN>;
    ## use critic
    chomp($resp);
    if ( $resp =~ m/(1|Yes|Ja|Y)/i ) {
        return 1;
    }
    elsif ( $resp =~ m/(a|All|alle)/i ) {
        return 2;
    }
    return;
}

sub ask_select {
    my ( $self, $msg, @options ) = @_;

    # let user select on of the options provided
    while (1) {
        print $msg. "\n";
        my $i = 0;
        foreach my $opt (@options) {
            print "[$i] $opt\n";
            $i++;
        }
        my $num = ask_number( 'Print enter any number between 0 and ' . $i . '. Press enter to abort' );
        if ( defined($num) && $options[$num] ) {
            return $options[$num];
        }
        else {
            return;
        }
    }

    return;
}

sub ask_number {
    my $self = shift;
    my $msg = shift;
    print $msg. ': ';
    ## no critic (ProhibitExplicitStdin)
    my $resp = <STDIN>;
    ## use critic
    chomp($resp);
    if ( $resp =~ m/^\s*(\d+)\s*$/ ) {
        return $1;
    }
    return;
}

sub ask_string {
    my $self = shift;
    my $msg = shift;
    print $msg. ': ';
    ## no critic (ProhibitExplicitStdin)
    my $resp = <STDIN>;
    ## use critic
    chomp($resp);
    return $resp;
}

no Moose;
__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

MTK::Cmd::Command - baseclass for any MTK CLI command

=head1 DESCRIPTION

Base class for any MTK CLI command.

=method ask_number

Helper to ask for a number on STDIN.

=method ask_select

Helper to ask for the selection of a given set of options from STDIN.

=method ask_string

Helper to ask for a string on STDIN.

=method ask_yesno

Helper to ask for a true/false on STDIN.

=method ask_yesnoall

Helper to ask for a true/false/all on STDIN.

=head1 ACKNOWLEDGEMENT

This module was originally developed for eGENTIC Systems. With approval from eGENTIC Systems,
this module was generalized and published, for which the authors would like to express their
gratitude.

=cut
