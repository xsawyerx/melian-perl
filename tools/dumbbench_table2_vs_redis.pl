#!/usr/bin/env perl
use strict;
use warnings;
use Getopt::Long qw(GetOptions);
use List::Util qw(first);
use Time::HiRes qw(time);
use JSON::XS qw< decode_json encode_json >;
use Redis;
use Melian;
use Dumbbench;

my %opt = (
    dsn         => 'unix:///tmp/melian.sock',
    schema_spec => undef,
    redis       => '127.0.0.1:6379',
    prefix      => 'table2:id:',
    rows        => 10_000,
    samples     => 3,
);

GetOptions(
    'dsn=s'         => \$opt{dsn},
    'schema_spec=s' => \$opt{schema_spec},
    'redis=s'       => \$opt{redis},
    'prefix=s'      => \$opt{prefix},
    'rows=i'        => \$opt{rows},
    'samples=i'     => \$opt{samples},
) or die "Usage: $0 [--dsn unix://sock | tcp://host:port] [--schema_spec spec] [--redis host:port] [--prefix keyprefix] [--rows N] [--samples N]\n";

$opt{rows} > 0 or die "Row count must be positive\n";

my $bench = Dumbbench->new(
  'target_rel_precision' => 0.005, # seek ~0.5%
  'initial_runs'         => 20,    # the higher the more reliable
);

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

my $max = 5; # 1e1;
$bench->add_instances(
    Dumbbench::Instance::PerlSub->new(
        'name' => 'Redis',
        'code' => sub {
            for ( 1 .. $max ) {
                for my $id ( 1 .. $opt{'rows'} ) {
                    decode_json(
                        $melian->fetch_raw(
                            $table2->{'id'}, $id_index->{'id'}, pack 'V', $id
                        )
                    );
                }
            }
        }
    ),

    Dumbbench::Instance::PerlSub->new(
        'name' => 'Melian',
        'code' => sub {
            for ( 1 .. $max ) {
                for my $id ( 1 .. $opt{'rows'} ) {
                    decode_json(
                        $redis->get( $opt{prefix} . $id )
                    );
                }
            }
        }
    ),
);

$bench->run;
$bench->report;

sub fetch_melian {
    my ($id) = @_;
    return $melian->fetch_raw( $table2->{'id'}, $id_index->{'id'}, pack('V', $id) );
}

sub fetch_redis {
    my ($id) = @_;
    my $payload = $redis->get( $opt{prefix} . $id );
    defined $payload or die sprintf "Missing Redis key %s%d", $opt{prefix}, $id;
    return $payload;
}

sub bench {
    my ( $label, $fetch ) = @_;
    my @lat_us;
    $lat_us[$opt{rows} - 1] = 0;
    my $start = time;
    for my $id ( 1 .. $opt{rows} ) {
        my $t0    = time;
        $fetch->($id);
        my $elapsed = time - $t0;
        $lat_us[ $id - 1 ] = $elapsed * 1_000_000;
    }
    my $elapsed = time - $start;
    my $rps     = $opt{rows} / $elapsed;
    my ($mean, $p95, $p99) = summarize_latency(\@lat_us);
    printf "%s: %d reqs in %.3f s → %.0f req/s, mean %.2f μs, P95 %.0f μs, P99 %.0f μs\n",
        $label, $opt{rows}, $elapsed, $rps, $mean, $p95, $p99;
}

sub summarize_latency {
    my ($samples) = @_;
    my $count = scalar @$samples;
    return (0, 0, 0) if $count == 0;

    my $sum = 0;
    $sum += $_ for @$samples;
    my $mean = $sum / $count;

    my @sorted = sort { $a <=> $b } @$samples;
    my $p95 = $sorted[ int( 0.95 * ($count - 1) ) ];
    my $p99 = $sorted[ int( 0.99 * ($count - 1) ) ];
    return ($mean, $p95, $p99);
}

