#!/usr/bin/perl
# ABSTRACT: MySQL Toolkit CLI
# PODNAME: mtk.pl
use strict;
use warnings;

use MTK::Cmd;

my $Cmd = MTK::Cmd::->new();
$Cmd->run();

exit 1;

__END__

=head1 NAME

mtk - MySQL Toolkit CLI

=head1 ACKNOWLEDGEMENT

This module was originally developed for eGENTIC Systems. With approval from eGENTIC Systems,
this module was generalized and published, for which the authors would like to express their
gratitude.

=cut
