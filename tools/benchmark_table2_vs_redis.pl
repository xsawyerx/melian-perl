#!/usr/bin/env perl
use strict;
use warnings;
use Getopt::Long qw(GetOptions);
use List::Util qw(first);
use Time::HiRes qw(time);
use JSON::XS qw< decode_json encode_json >;
use Redis;
use Melian;

my %opt = (
    dsn         => 'unix:///tmp/melian.sock',
    schema_spec => undef,
    redis       => '127.0.0.1:6379',
    prefix      => 'table2:id:',
    rows        => 10_000,
    rounds      => 5,
    target_cv   => 5,
    samples     => 3,
);

GetOptions(
    'dsn=s'         => \$opt{dsn},
    'schema_spec=s' => \$opt{schema_spec},
    'redis=s'       => \$opt{redis},
    'prefix=s'      => \$opt{prefix},
    'rows=i'        => \$opt{rows},
    'rounds=i'      => \$opt{rounds},
    'target-cv=i'   => \$opt{target_cv},
    'samples=i'     => \$opt{samples},
) or die "Usage: $0 [--dsn unix://sock | tcp://host:port] [--schema_spec spec] [--redis host:port] [--prefix keyprefix] [--rows N] [--samples N]\n";

$opt{rows} > 0 or die "Row count must be positive\n";
$opt{rounds} > 0 or die "Rounds must be positive\n";
$opt{target_cv} > 0 or die "target_cv must be positive\n";

my $json = JSON::XS->new->canonical(1);

my $melian = Melian->new(
    'dsn'         => $opt{dsn},
    ($opt{schema_spec} ? ('schema_spec' => $opt{schema_spec}) : ()),
);
my $schema = $melian->{'schema'};
my ($table2) = grep { $_->{'name'} eq 'table2' } @{ $schema->{'tables'} }
    or die "Schema does not expose table2; pass --schema_spec if running offline\n";
my ($id_index) = first { $_->{'column'} eq 'id' } @{ $table2->{'indexes'} }
    or die "table2 is missing an id index\n";

my $redis = Redis->new( server => $opt{redis} );
$redis->ping;

sub fetch_melian {
    my ($id) = @_;
    return $melian->fetch_raw( $table2->{'id'}, $id_index->{'id'}, pack('V', $id) );
}

sub fetch_redis {
    my ($id) = @_;
    my $payload = $redis->get( $opt{prefix} . $id );
    #defined $payload or die sprintf "Missing Redis key %s%d", $opt{prefix}, $id;
    return $payload;
}

sub bench {
    my ( $label, $fetch ) = @_;
    my @lat_us;
    my $total = $opt{rows} * $opt{rounds};
    $lat_us[$total - 1] = 0;
    my $start = time;
    for my $round (1 .. $opt{rounds}) {
        for my $id ( 1 .. $opt{rows} ) {
            my $t0 = time;
            $fetch->($id);
            my $elapsed = time - $t0;
            $lat_us[ ($round - 1) * $opt{rows} + $id - 1 ] = $elapsed * 1_000_000;
        }
    }
    my $elapsed = time - $start;
    my $rps     = $total / $elapsed;
    my ($mean, $p95, $p99, $cv) = summarize_latency(\@lat_us, $opt{target_cv});
    printf "%s: %d reqs in %.3f s → %.0f req/s, mean %.2f μs (CV %.1f%%), P95 %.0f μs, P99 %.0f μs\n",
        $label, $total, $elapsed, $rps, $mean, $cv, $p95, $p99;
}

sub summarize_latency {
    my ($samples, $target_cv) = @_;
    my @clean = @$samples;
    my $cv = calc_cv(\@clean);
    while (@clean > 10 && $cv > $target_cv) {
        @clean = trim_outliers(\@clean);
        $cv = calc_cv(\@clean);
    }

    my $count = scalar @clean;
    return (0, 0, 0, 0) if $count == 0;

    my $sum = 0;
    $sum += $_ for @clean;
    my $mean = $sum / $count;

    my @sorted = sort { $a <=> $b } @clean;
    my $p95 = $sorted[ int( 0.95 * ($count - 1) ) ];
    my $p99 = $sorted[ int( 0.99 * ($count - 1) ) ];
    return ($mean, $p95, $p99, $cv);
}

sub calc_cv {
    my ($samples) = @_;
    my $count = scalar @$samples;
    return 0 if $count < 2;
    my $sum = 0;
    $sum += $_ for @$samples;
    my $mean = $sum / $count;
    return 0 if $mean == 0;
    my $variance = 0;
    $variance += ($_ - $mean) ** 2 for @$samples;
    $variance /= ($count - 1);
    my $stddev = sqrt($variance);
    return ($stddev / $mean) * 100;
}

sub trim_outliers {
    my ($samples) = @_;
    my @sorted = sort { $a <=> $b } @$samples;
    my $count = @sorted;
    return @$samples if $count < 4;
    my $trim = int($count * 0.05) || 1;
    return @sorted[$trim .. $count - $trim - 1];
}

my @check_ids;
if ( $opt{samples} > 0 ) {
    @check_ids = ( 1, int( $opt{rows} / 2 ), $opt{rows} );
    @check_ids = @check_ids[ 0 .. $opt{samples} - 1 ] if @check_ids > $opt{samples};
    printf "Verifying %d sample ids ...\n", scalar @check_ids;
    for my $id (@check_ids) {
        my $melian_payload = fetch_melian($id);
        my $redis_payload  = fetch_redis($id);
        my $melian_obj     = $json->decode($melian_payload);
        my $redis_obj      = $json->decode($redis_payload);
        my $melian_norm    = $json->encode($melian_obj);
        my $redis_norm     = $json->encode($redis_obj);
        if ( $melian_norm ne $redis_norm ) {
            die sprintf "Mismatch for id %d\nMelian: %s\nRedis : %s\n",
                $id, $melian_norm, $redis_norm;
        }
    }
    print "Sample rows match between Melian and Redis.\n\n";
}

bench( 'Melian (-C equivalent)', \&fetch_melian );
bench( 'Redis (GET)',             \&fetch_redis );
