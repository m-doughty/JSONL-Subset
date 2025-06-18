package JSONL::Subset;

use strict;
use warnings;

use Exporter 'import';
use IO::File;
use List::Util qw(shuffle);

our @EXPORT_OK = qw(subset_jsonl);

sub subset_jsonl {
    my %args = @_;
    my ($infile, $outfile, $percent, $mode, $seed, $streaming) =
        @args{qw/infile outfile percent mode seed streaming/};

    die "infile, outfile, and percent are required" unless $infile && $outfile && defined $percent;
    die "percent must be between 0 and 100" unless $percent > 0 && $percent <= 100;
    die "Invalid mode: $mode" unless $mode =~ /^(random|start|end)$/;

    $mode ||= 'random';

    if (!defined $streaming || $streaming == 0) {
        _subset_jsonl_inplace(
            infile => $infile,
            outfile => $outfile,
            percent => $percent,
            mode => $mode,
            seed => $seed
        );
    } else {
        _subset_jsonl_streaming(
            infile => $infile,
            outfile => $outfile,
            percent => $percent,
            mode => $mode,
            seed => $seed
        );
    }
}

sub _subset_jsonl_inplace {
    my %args = @_;
    my ($infile, $outfile, $percent, $mode, $seed) =
        @args{qw/infile outfile percent mode seed/};

    my $in = IO::File->new($infile, "<:encoding(UTF-8)") or die "Can't read $infile: $!";
    my @lines = grep { /\S/ } map { chomp; $_ } <$in>;

    if ($mode eq 'random') {
        srand($seed) if defined $seed;
        @lines = shuffle(@lines);
    }

    my $count = int(@lines * $percent / 100);
    my @subset = $mode eq 'end'  
                 ? @lines[-$count..-1]
                 : @lines[0..$count-1];
    my $out = IO::File->new($outfile, ">:encoding(UTF-8)") or die $!;

    for my $el (@subset) {
        chomp $el;

        $out->print("$el\n");
    }
    $out->close;

    $in->close;
}

sub _subset_jsonl_streaming {
    my %args = @_;
    my ($infile, $outfile, $percent, $mode, $seed) =
        @args{qw/infile outfile percent mode seed/};

    my $in = IO::File->new($infile, "<:encoding(UTF-8)") or die "Can't read $infile: $!";
    my $total = 0;

    while (my $line = <$in>) {
        $total++ if $line =~ /\S/;
    }
    close $in;

    my $count = int($total * $percent / 100);
    my @all_indexes = (0..$total-1);

    if ($mode eq 'random') {
        srand($seed) if defined $seed;
        @all_indexes = shuffle(@all_indexes);
    }

    my @picked = $mode eq 'end'  
                 ? @all_indexes[-$count..-1]
                 : @all_indexes[0..$count-1];
    my %picked = map { $_ = 1 } @picked;

    open $in, "<:encoding(UTF-8)", $infile or die $!;
    open my $out, ">:encoding(UTF-8)", $outfile or die $!;
    my $real = 0;

    while (my $line = <$in>) {
        next unless $line =~ /\S/;
        chomp $line;

        print $out "$line\n" if $picked{$real};
        $real++;

        if ($mode eq 'start' && $real >= $count) {
            last;
        }
    }

    close $in;
    close $out;
}

1;

__END__

=head1 NAME

JSONL::Subset - Extract a percentage of lines from a JSONL file

=head1 SYNOPSIS

  use JSONL::Subset qw(subset_jsonl);

  subset_jsonl(
      infile  => "data.jsonl",
      outfile => "subset.jsonl",
      percent => 10,
      mode    => "random",  # or "start", "end"
      seed    => 42
  );

=head1 DESCRIPTION

This module helps you extract a subset of lines from a JSONL file, for sampling or inspection.

=head1 OPTIONS

=head2 infile

Path to the file you want to import from.

=head2 outfile

Path to where you want to save the export.

=head2 percent

Percentage of lines to retain.

=head2 mode

- random returns random lines
- start returns lines from the start
- end returns lines from the end

=head2 seed

Only used with random, for reproducability. (optional)

=head2 streaming

If set, infile will be streamed line by line. This makes the process take less RAM, but more wall time.

Recommended for large JSONL files.

=cut
