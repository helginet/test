#!/usr/bin/perl -W

use strict;
use warnings 'all';
use utf8;

use Data::Dumper;
use IO::Socket;

$|++;

my $listen_socket = IO::Socket::INET->new(
	LocalPort => 9000,
	Listen    => 10,
	Proto     => 'tcp',
	Reuse     => 1
);

while () {
	sleep(3);
};
