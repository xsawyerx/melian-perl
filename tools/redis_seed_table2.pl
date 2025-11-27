use strict;
use warnings;
use Getopt::Long qw(GetOptions);
use JSON::XS;
use Redis;

my %opt = (
    redis  => '127.0.0.1:6379',
    rows   => 10_000,
    prefix => 'table2:id:',
    flush  => 0,
);

GetOptions(
    'redis=s'  => \$opt{redis},
    'rows=i'   => \$opt{rows},
    'prefix=s' => \$opt{prefix},
    'flush'    => \$opt{flush},
) or die "Usage: $0 [--redis host:port] [--rows N] [--prefix keyprefix] [--flush]\n";

$opt{rows} > 0
    or die "Row count must be positive\n";

my $redis = Redis->new( server => $opt{redis} );
$redis->ping;

if ( $opt{flush} ) {
    my @keys = map { $opt{prefix} . $_ } 1 .. $opt{rows};
    while (@keys) {
        my @chunk = splice( @keys, 0, 1024 );
        $redis->del(@chunk);
    }
}

my $json = JSON::XS->new->canonical(1);
my @statuses = qw(active inactive maintenance);

for my $id ( 1 .. $opt{rows} ) {
    my $hostname = sprintf 'host-%05u', $id;
    my $ip       = sprintf '10.%u.%u.%u', int($id / 256), $id % 256, int($id / 10) % 255;
    my $status   = $statuses[ $id % @statuses ];

    my $payload = $json->encode(
        {
            id       => $id,
            hostname => $hostname,
            ip       => $ip,
            status   => $status,
        }
    );

    $redis->set( $opt{prefix} . $id => $payload );
}

printf "Seeded %d Redis keys with prefix '%s'\n", $opt{rows}, $opt{prefix};
