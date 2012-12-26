#!perl -T

use Test::More tests => 5;

BEGIN {
    use_ok( 'MTK::Cmd::Command::delay' ) || print "Bail out!
";
    use_ok( 'MTK::Cmd::Command::doall' ) || print "Bail out!
";
    use_ok( 'MTK::Cmd::Command::heartbeat' ) || print "Bail out!
";
    use_ok( 'MTK::Cmd::Command' ) || print "Bail out!
";
    use_ok( 'MTK::Cmd' ) || print "Bail out!
";
}

diag( "Testing MTK::Cmd $MTK::Cmd::VERSION, Perl $], $^X" );
