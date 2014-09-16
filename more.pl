#!/usr/bin/perl -W

use strict;
use warnings;

use utf8;

use Data::Dumper;
use Sort::External;

$|++;

my $fn1 = sort_file($ARGV[0]);
my $fn2 = sort_file($ARGV[1]);


my $result = $ARGV[2];

unlink($result);

open(my $f1, '<', $fn1);
open(my $f2, '<', $fn2);

my $skip;
my $l2;
my $go1 = 1;
my $go2 = 1;
my $l1;
while (1) {
    if ($go1) {
        last unless defined($l1 = <$f1>);
        $l1 =~ s![\r\n]!!g;
    }
    my $exist;
    unless ($skip) {
        while (1) {
            if ($go2) {
                last unless defined($l2 = <$f2>);
            }
            $l2 =~ s![\r\n]!!g;
            my $res = $l1 cmp $l2;
            print "$l1 $l2 $res\n";
            if ($res == 0) {
                $exist++;
                $go1 = 1;
                $go2 = 1;
                last;
            } elsif ($res == -1) {
                $go2 = 0;
                $go1 = 1;
                last;
            } else {
                $go1 = 0;
                $go2 = 1;
                $exist++;
                last;
            }
        };
    }
    unless ($exist) {
        open(my $f3, '>>', $result);
        print $f3 $l1 . "\n";
        close($f3);
    }
};

close($f2);
close($f1);

sub sort_file {
    my ($file) = @_;
    my $sortex = Sort::External->new(mem_threshold => 1024**2 * 512);
    open(my $f, '<', $file);
    while (<$f>) {
        $sortex->feed($_);
    };
    $sortex->finish(outfile => $file . '.sorted');
    return $file . '.sorted';
};
