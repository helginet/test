#!/usr/bin/perl -W

use strict;
use warnings;
use utf8;

$|++;

if (scalar(@ARGV) < 3) {
    print "Usage: compare.pl <file1>  <file2> <file3>\n";
    exit;
}

system('rm -rf ./compare/*; rm -f ./' . $ARGV[2]); # this is not necessary, but useful while testing

my $first        = 1;
my $lines_passed = 0;

open(my $f1, '<', $ARGV[0]) or die("Can't open file: " . $ARGV[0]);
while (my $line = <$f1>) {
    $line     =~ s![\r\n]!!g;
    my $exist = check($line);
    unless ($exist) {
        open(my $f3, '>>', $ARGV[2]) or die("Can't open file: " . $ARGV[2]);
        print $f3 $line . "\n";
        close($f3);
    }
    $lines_passed++;
    if ($lines_passed =~ m!00$!) {
        print "Lines passed: $lines_passed\n";
    }
};
close($f1);

sub check {
    my ($line) = @_;
    my $f2;
    if ($first) {
        open($f2, '<', $ARGV[1]) or die("Can't open file: " . $ARGV[1]);
    } else {
        my $slug = make_slug($line);
        if (-e "./compare/$slug") {
            open($f2, '<', "./compare/$slug") or die("Can't open file: " . "./compare/$slug");
        } else {
            return 0;
        }
    }
    while (my $string = <$f2>) {
        $string =~ s![\r\n]!!g;
        if ($first) {
            my $cur_slug = make_slug($string);
            open(my $fslug, '>>', "./compare/$cur_slug") or die("Can't open file: " . "./compare/$cur_slug");
            print $fslug $string . "\n";
            close($fslug);
        }
        if ($string eq $line) {
            close($f2);
            return 1;
        }
    };
    close($f2);
    $first-- if $first;
    return 0;
};

sub make_slug {
    my ($line) = @_;
    my ($slug) = $line =~ m!^(.{1,20})!; # this can be tuned based on files sizes
    $slug ||= '_';
    my @slug_chars = $slug =~ m!(.)!g;
    foreach (@slug_chars) {
        $_ = ord;
    };
    $slug = join('_', @slug_chars);
    return $slug;
};
