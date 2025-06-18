use strict;
use warnings;

use File::Temp qw(tempfile);
use Test::More tests => 13;
use JSONL::Subset qw(subset_jsonl);

my $FIXTURE = "t/fixtures/sample.jsonl";

ok(defined &subset_jsonl, 'subset_jsonl is defined');

# Start mode
my ($fh_out_s, $filename_out_s) = tempfile();
subset_jsonl(
	infile => $FIXTURE,
	outfile => $filename_out_s,
	percent => 30,
	mode => "start"
);
open my $s, "<", $filename_out_s or die $!;
my @start_out = <$s>;
close $s;
is(scalar(@start_out), 3, "start: got exactly 3 lines");
is_deeply(\@start_out, ["{ \"id\": 1 }\n", "{ \"id\": 2 }\n", "{ \"id\": 3 }\n"], "start: got the right lines");

# End mode
my ($fh_out_e, $filename_out_e) = tempfile();
subset_jsonl(
	infile => $FIXTURE,
	outfile => $filename_out_e,
	percent => 30,
	mode => "end"
);
open my $e, "<", $filename_out_e or die $!;
my @end_out = <$e>;
close $e;
is(scalar(@end_out), 3, "end: got exactly 3 lines");
is_deeply(\@end_out, ["{ \"id\": 8 }\n", "{ \"id\": 9 }\n", "{ \"id\": 10 }\n"], "end: got the right lines");

# Random mode
my ($fh_out_r, $filename_out_r) = tempfile();
subset_jsonl(
	infile => $FIXTURE,
	outfile => $filename_out_r,
	percent => 30,
	mode => "random",
	seed => 1337
);
open my $r, "<", $filename_out_r or die $!;
my @rand_out = <$r>;
close $r;
is(scalar(@rand_out), 3, "random: got exactly 3 lines");
is_deeply(\@rand_out, ["{ \"id\": 9 }\n", "{ \"id\": 6 }\n", "{ \"id\": 7 }\n"], "random: got the right lines");

# Start mode (streaming)
my ($fh_out_ss, $filename_out_ss) = tempfile();
subset_jsonl(
	infile => $FIXTURE,
	outfile => $filename_out_ss,
	percent => 30,
	mode => "start"
);
open my $ss, "<", $filename_out_ss or die $!;
my @start_out_s = <$ss>;
close $ss;
is(scalar(@start_out_s), 3, "start: got exactly 3 lines");
is_deeply(\@start_out_s, ["{ \"id\": 1 }\n", "{ \"id\": 2 }\n", "{ \"id\": 3 }\n"], "start: got the right lines");

# End mode
my ($fh_out_es, $filename_out_es) = tempfile();
subset_jsonl(
	infile => $FIXTURE,
	outfile => $filename_out_es,
	percent => 30,
	mode => "end"
);
open my $es, "<", $filename_out_es or die $!;
my @end_out_s = <$es>;
close $es;
is(scalar(@end_out_s), 3, "end: got exactly 3 lines");
is_deeply(\@end_out_s, ["{ \"id\": 8 }\n", "{ \"id\": 9 }\n", "{ \"id\": 10 }\n"], "end: got the right lines");

# Random mode
my ($fh_out_rs, $filename_out_rs) = tempfile();
subset_jsonl(
	infile => $FIXTURE,
	outfile => $filename_out_rs,
	percent => 30,
	mode => "random",
	seed => 1337
);
open my $rs, "<", $filename_out_rs or die $!;
my @rand_out_s = <$rs>;
close $rs;
is(scalar(@rand_out_s), 3, "random: got exactly 3 lines");
is_deeply(\@rand_out_s, ["{ \"id\": 9 }\n", "{ \"id\": 6 }\n", "{ \"id\": 7 }\n"], "random: got the right lines");
