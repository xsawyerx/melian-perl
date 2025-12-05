package Melian;
# ABSTRACT: Perl client to the Melian cache

use v5.34;
use Carp qw(croak);
use IO::Socket::INET;
use IO::Socket::UNIX;
use JSON::XS qw(decode_json);
use List::Util qw(first);
use Socket qw(SOCK_STREAM);
use constant {
    'MELIAN_HEADER_VERSION' => 0x11,
    'ACTION_FETCH'          => ord('F'),
    'ACTION_DESCRIBE'       => ord('D'),
};

use Exporter qw(import);
our @EXPORT_OK = qw(
    fetch_raw_with
    fetch_by_int_with
    fetch_by_string_with
    table_id_of
    column_id_of
);

sub new {
    my ($class, @opts) = @_;
    @opts > 0 && @opts % 2 == 0
        or croak('Melian->new(%args)');

    my %args = @opts;
    my $self = bless {
        'dsn'     => $args{'dsn'}     // 'unix:///tmp/melian.sock',
        'timeout' => $args{'timeout'} // 1,
        'socket'  => undef,
    }, $class;

    my @schema_args = grep /^schema(?:_spec|_file)?$/, keys %args;
    @schema_args > 1
        and croak(q{Provide maximum one of: 'schema', 'schema_spec', 'schema_file'});

    if ( ! $self->{'socket'} ) {
        $self->connect();
    }

    if (my $schema = $args{'schema'}) {
        $self->{'schema'} = $schema;
    } elsif (my $spec = $args{'schema_spec'}) {
        $self->{'schema'} = _load_schema_from_spec( $args{'schema_spec'} );
    } elsif (my $path = $args{'schema_file'}) {
        $self->{'schema'} = _load_schema_from_file( $args{'schema_file'} );
    } else {
        $self->{'schema'} = $self->_load_schema_from_describe();
    }

    return $self;
}

sub create_connection {
    my ($class, @opts) = @_;
    @opts > 0 && @opts % 2 == 0
        or croak('Melian->create_connection(%args)');

    my %args    = @opts;
    my $dsn     = $args{'dsn'}     // 'unix:///tmp/melian.sock';
    my $timeout = $args{'timeout'} // 1;

    return _connect_return_socket( $dsn, $timeout );
}

sub connect {
    my ($self) = @_;
    return if $self->{'socket'};

    my $dsn = $self->{'dsn'};
    if ($dsn =~ m{^unix://(.+)$}i) {
        my $path = $1;
        $self->{'socket'} = IO::Socket::UNIX->new(
            'Type' => SOCK_STREAM(),
            'Peer' => $path,
        ) or croak "Failed to connect to UNIX socket $path: $!";
    } elsif ($dsn =~ m{^tcp://([^:]+):(\d+)$}i) {
        my ($host, $port) = ($1, $2);
        $self->{'socket'} = IO::Socket::INET->new(
            'PeerHost' => $host,
            'PeerPort' => $port,
            'Proto'    => 'tcp',
            'Timeout'  => $self->{'timeout'},
        ) or croak "Failed to connect to $host:$port: $!";
    } else {
        croak "Unsupported DSN '$dsn'. Use unix:///path or tcp://host:port";
    }

    $self->{'socket'}->autoflush(1);
    return 1;
}

sub _connect_return_socket {
    my $socket;
    if ($_[0] =~ m{^unix://(.+)$}i) {
        my $path = $1;
        $socket = IO::Socket::UNIX->new(
            'Type' => SOCK_STREAM(),
            'Peer' => $path,
        ) or croak "Failed to connect to UNIX socket $path: $!";
    } elsif ($_[0]=~ m{^tcp://([^:]+):(\d+)$}i) {
        my ($host, $port) = ($1, $2);
        $socket = IO::Socket::INET->new(
            'PeerHost' => $host,
            'PeerPort' => $port,
            'Proto'    => 'tcp',
            'Timeout'  => $_[1],
        ) or croak "Failed to connect to $host:$port: $!";
    } else {
        croak "Unsupported DSN '$_[0]'. Use unix:///path or tcp://host:port";
    }

    $socket->autoflush(1);
    return $socket;
}

sub disconnect {
    my ($self) = @_;
    if ($self->{'socket'}) {
        $self->{'socket'}->close();
        $self->{'socket'} = undef;
        return 1;
    }

    return;
}

sub disconnect_socket {
    $_[0] or return;
    $_[0]->close();
    $_[0] = undef;
    return 1;
}

sub fetch_raw_from {
    my ( $self, $table_name, $column_name, $key ) = @_;

    # Get table ID
    my $table = $self->get_table_id($table_name);
    my $table_id = $table->{'id'};

    # Get column ID
    my $column_id = $self->get_column_id( $table, $column_name );
    return $self->fetch_raw( $table_id, $column_id, $key );
}

sub fetch_raw {
    my ( $self, $table_id, $column_id, $key ) = @_;
    defined $key
        or croak("You must provide a key to fetch");
    return $self->_send(ACTION_FETCH(), $table_id, $column_id, $key);
}

# $conn, $table_id, $column_id, $key
sub fetch_raw_with {
    defined $_[3]
        or croak("You must provide a key to fetch");
    return _send_with($_[0], ACTION_FETCH(), $_[1], $_[2], $_[3]);
}

sub fetch_by_string_from {
    my ( $self, $table_name, $column_name, $key ) = @_;

    # Get table ID
    my $table = $self->get_table_id($table_name);
    my $table_id = $table->{'id'};

    # Get column ID
    my $column_id = $self->get_column_id( $table, $column_name );
    return $self->fetch_by_string( $table_id, $column_id, $key );
}

sub fetch_by_string {
    my ($self, $table_id, $column_id, $key) = @_;
    my $payload = $self->fetch_raw($table_id, $column_id, $key);
    return undef if $payload eq '';
    return decode_json($payload);
}

# $conn, $table_id, $column_id, $key
sub fetch_by_string_with {
    my $payload = fetch_raw_with($_[0], $_[1], $_[2], $_[3]);
    return undef if $payload eq '';
    return decode_json($payload);
}

sub fetch_by_int_from {
    my ( $self, $table_name, $column_name, $id ) = @_;

    # Get table ID
    my $table = $self->get_table_id($table_name);
    my $table_id = $table->{'id'};

    # Get column ID
    my $column_id = $self->get_column_id( $table, $column_name );

    return $self->fetch_by_int( $table_id, $column_id, $id );
}

sub fetch_by_int {
    my ($self, $table_id, $column_id, $id) = @_;
    return $self->fetch_by_string($table_id, $column_id, pack('V', $id));
}

# $conn, $table_id, $column_id, $id
sub fetch_by_int_with {
    return fetch_by_string_with($_[0], $_[1], $_[2], pack('V', $_[3]));
}

sub _load_schema_from_describe {
    my $self = shift;

    my $payload = $self->_send(ACTION_DESCRIBE(), 0, 0, '');
    defined $payload && length $payload
        or croak('Could not get schema data');

    return decode_json($payload);
}


sub _load_schema_from_file {
    my $path = shift;

    open my $fh, '<', $path
        or croak("Cannot open schema file $path: $!");
    local $/;
    my $content = <$fh>;
    close $fh
        or croak("Cannot close schema file: $path: $!");

    my $decoded;
    eval {
        $decoded = decode_json($content);
        1;
    } or do {
        my $error = $@ || 'Zombie error';
        croak("Failed to parse JSON schema in file '$path': $error");
    };

    return $decoded;
}

# table1#0|60|id:int,table2#1|45|id:int;hostname:string
sub _load_schema_from_spec {
    my $spec = shift;
    my %data;

    for my $section_data ( split m{,}, $spec ) {
        my ( $table_data, $table_period, $columns ) = split m{\|}, $section_data;
        my ( $table_name, $table_id ) = split m{#}, $table_data;
        defined $table_name && defined $table_id
            or croak('Schema spec failure: Missing table name or table ID');

        my %table_entry = (
            'name'   => $table_name,
            'id'     => $table_id,
            'period' => $table_period,
        );

        my @columns;
        my $column_id = 0;
        foreach my $column_data ( split m{;}, $columns ) {
            my ( $column_name, $column_type ) = split /:/, $column_data;
            push @{ $table_entry{'indexes'} }, {
                'id'     => $column_id++,
                'column' => $column_name,
                'type'   => $column_type,
            }
        }

        push @{ $data{'tables'} }, \%table_entry;
    }

    return \%data;
}

sub _send {
    my ( $self, $action, $table_id, $column_id, $payload ) = @_;
    $payload //= '';
    defined $table_id && defined $column_id
        or croak("Invalid table ID or index ID");

    my $header = pack(
        'CCCCN',
        MELIAN_HEADER_VERSION(),
        $action,
        $table_id,
        $column_id,
        length $payload,
    );

    _write_all( $self->{'socket'}, $header . $payload );
    my $len_buf = _read_exactly( $self->{'socket'}, 4 );
    my $len = unpack 'N', $len_buf;
    return '' if $len == 0;
    return _read_exactly( $self->{'socket'}, $len );
}

# $conn, $action, $table_id, $column_id, $payload
sub _send_with {
    $_[4] //= '';
    defined $_[2] && defined $_[3]
        or croak("Invalid table ID or index ID");

    my $header = pack(
        'CCCCN',
        MELIAN_HEADER_VERSION(),
        $_[1],
        $_[2],
        $_[3],
        length $_[4],
    );

    _write_all( $_[0], $header . $_[4] );
    my $len_buf = _read_exactly( $_[0], 4 );
    my $len = unpack 'N', $len_buf;
    return '' if $len == 0;
    return _read_exactly( $_[0], $len );
}

# $socket, $buf
sub _write_all {
    my $offset = 0;
    my $len = length $_[1];
    while ( $offset < $len ) {
        my $written = syswrite( $_[0], $_[1], $len - $offset, $offset );
        croak("Melian write failed: $!") unless defined $written && $written > 0;
        $offset += $written;
    }
}

# $socket, $len
sub _read_exactly {
    my $buffer = '';

    while ( length($buffer) < $_[1] ) {
        my $read = sysread( $_[0], my $chunk, $_[1] - length $buffer );
        croak("Melian read failed: $!") unless defined $read;
        croak("Melian socket closed unexpectedly") if $read == 0;
        $buffer .= $chunk;
    }

    return $buffer;
}

sub get_table_id {
    my ( $self, $name ) = @_;
    my $table = List::Util::first(
        sub { $_->{'name'} eq $name },
        @{ $self->{'schema'}{'tables'} },
    );

    $table or croak("Cannot find table named '$name'");
    return $table;
}

sub table_id_of {
    my ( $schema, $name ) = @_;
    my $table = List::Util::first(
        sub { $_->{'name'} eq $name },
        @{ $schema->{'tables'} }
    );
    $table or croak("Cannot find table named '$name'");
    return $table;
}

sub get_column_id {
    my ( $self, $table, $name ) = @_;

    # Get column ID
    my $column = List::Util::first(
        sub { $_->{'column'} eq $name },
        @{ $table->{'indexes'} },
    );

    $column or croak("Cannot find column named '$name'");
    return $column->{'id'};
}

sub column_id_of {
    my ( $table, $name ) = @_;

    # Get column ID
    my $column = List::Util::first(
        sub { $_->{'column'} eq $name },
        @{ $table->{'indexes'} },
    );

    $column or croak("Cannot find column named '$name'");
    return $column->{'id'};
}


sub DESTROY { $_[0]->disconnect() }

1;

__END__

=head1 SYNOPSIS

    use Melian;

    # Object-oriented interface
    my $client = Melian->new(
        dsn => 'unix:///tmp/melian.sock',
    );

    # With names
    $client->fetch_by_string_from( 'cats', 'name', 'Pixel' );

    # With IDs
    my $row = $client->fetch_by_string(1, 1, 'Pixel');

    # Functional interface (much faster!)
    use Melian qw< fetch_by_string_with >;
    my $conn = Melian->create_connection(
        'dsn'     => '...',
        'timeout' => 1,
    );

    # With IDs (cannot do with names)
    my $row = fetch_by_string_with( $conn, 1, 1, 'Pixel' );

=head1 DESCRIPTION

C<Melian> provides a Perl client for the Melian cache server. It handles the
binary protocol, schema negotiation, and simple fetch helpers so applications
can retrieve rows by table/index names or identifiers using either UNIX or TCP
sockets.

There are three ways to lookup data with Melian:

=over 4

=item * OO: Using names for the table and column

See C<fetch_raw_from()>, C<fetch_by_int_from()>, and C<fetch_by_string_from()>
under B<METHODS> below.

In this case, Melian will figure out the IDs for you, and will request it
using those IDs.

=item * OO: Using the IDs for the table and column.

This is faster, but requires you to get the table and column IDs. You can
retrieve them through C<< $melian->{'schema'} >> and keep them and avoid
Melian having to look it up each time.

See C<fetch_raw()>, C<fetch_by_int()>, and C<fetch_by_string()> under
B<METHODS> below.

=item * Functional: using the IDs for the table and column

This is even faster, but less pretty. Perl's OO capability still has some
overhead you can avoid by using the fully functional interface.

See C<fetch_raw_with()>, C<fetch_by_int_with()>, and C<fetch_by_string_with()>
under B<FUNCTIONS> below.

(There is no functional interface that uses the table name and column, since
we're optimizing for full speed.)

=back

=head1 SCHEMA

Melian uses a table definition schema. This includes information about the
table(s) you have (names, ID), how often you want them refreshed, and which
columns in those tables you want to use for lookups (their names, IDs, and
type).

=head2 Single-table single-column example

The schema C<people#0|60|id#0:int> says:

=over 4

=item * table name:         people

=item * table ID:           0

=item * refresh:            every 60 seconds

=item * lookup column:      'id'

=item * lookup column id:   0

=item * lookup column type: int

=back

We can use the OO interface:

    my $melian = Melian->new(...);

    # Using the table and column names, asking for ID "20"
    $melian->fetch_by_int_from( 'people', 'id', 20 );

    # Using the IDs for table and column (which is faster)
    $melian->fetch_by_int( 0, 0, 20 );

Or we can use the functional interface:

    # This is much faster than OO
    use Melian qw< fetch_by_int_with >;
    my $conn = Melian->create_connection(...);
    fetch_by_int_with( $conn, 0, 0, 20 );

=head2 Multi-table multi-column example

The schema C<people#0|60|id#0:int,cats#1|45|id#0:int;name#1:string> says:

=over 4

=item * two tables: C<people> and C<cats>

=item * table C<people> has ID C<0>, table C<cats> has ID C<1>

=item * C<people> table has one look up column: C<id>

=item * C<people>'s C<id> column has ID C<0> and type C<int>

=item * C<cats> table has two lookup columns: C<id> and C<name>

=item * C<cat>'s C<id> column has ID C<0> and type C<int>

=item * C<cat>'s C<name> column has ID C<1> and type C<string>

=back

We can use the OO interface:

    my $melian = Melian->new(...);

    # Using the table and column names
    $melian->fetch_by_int_from( 'people', 'id', 20 );
    $melian->fetch_by_int_from( 'cats', 'id', 10 );
    $melian->fetch_by_string_from( 'cats', 'name', 'Pixel' );

    # Using the IDs for table and column (which is faster)
    $melian->fetch_by_int( 0, 0, 20 );
    $melian->fetch_by_int( 1, 0, 10 );
    $melian->fetch_by_int( 1, 1, 'Pixel' );

Or we can use the functional interface:

    # This is much faster than OO
    use Melian qw< fetch_by_int_with fetch_by_string_with >;
    my $conn = Melian->create_connection(...);
    fetch_by_int_with( $conn, 0, 0, 20 );
    fetch_by_int_with( $conn, 1, 0, 10 );
    fetch_by_string_with( $conn, 1, 1, 'Pixel' );

=head1 METHODS

These are the object-oriented Melian interface methods. It is not as fast as
the functional interface described below under B<FUNCTIONS>.

=head2 new

    # Using TCP/IP
    my $client = Melian->new(
        'dsn'         => 'tcp://127.0.0.1:8765',
        'schema_spec' => 'table1#0|60|id#0:int',
        'timeout'     => 1,
    );

    # Using UNIX sockets
    my $client = Melian->new(
        'dsn' => 'unix:///tmp/melian.sock',
        ...
    );

    # More complicated schema
    my $client = Melian->new(
        'schema_spec' => 'table1#0|60|id#0:int,table2#1|45|id#0:int;name#1:string',
        ...
    );

Creates a new client. Require a C<dsn> and optionally accept C<timeout>,
C<schema>, C<schema_spec>, or C<schema_file> to control how the schema is
loaded.

The C<timeout> is used for TPC/IP connections, the C<schema> is used to lookup
the table and column IDs when using C<fetch_raw_from()>, C<fetch_by_int_from()>,
and C<fetch_by_string_from()>.

Logic for handling schema, in the following order of priority:

=over 4

=item * If you provide a C<schema> attribute, it uses it.

=item * If you provide a C<schema_file> attribute, it will parse it.

=item * If you provide a C<schema_spec> attribute, it will parse the spec.

=item * If you provide none, will request the schema from the server.

=back

=head2 connect

    $client->connect();

Explicitly opens the underlying socket. Called automatically on C<new()>. You
do not need to call it, unless you explicitly called C<disconnect()> and want
to reconnect.

=head2 disconnect

    $client->disconnect();

Closes the socket connection. Happens automatically when using the OO version
and the variable goes out of scope.

=head2 C<fetch_raw($table_id, $column_id, $key_bytes)>

    # table id 0, column id 1
    my $json_data = $client->fetch_raw( 1, 1, 'Pixel' );

Looks up a value using table ID, column ID, and a key.

For a name-friendly version, see C<fetch_raw_from()>.

=head2 C<fetch_raw_from($table_name, $column_name, $key_bytes)>

    # table name 'cats', column name 'name'
    my $json_data = $client->fetch_raw_from( 'cats', 'name', 'Pixel' )

Similar to C<fetch_raw()> but allows you to use the table and column names.

C<fetch_raw()> is faster.

=head2 C<fetch_by_string($table_id, $column_id, $string_key)>

    my $row = $client->fetch_by_string( 1, 1, 'Pixel' );

Like C<fetch_raw> but decodes the JSON payload into a hashref, or returns
C<undef> if the server responds with an empty payload.

For a name-friendly version, see C<fetch_by_string_from()>.

=head2 C<fetch_by_string_from($table_name, $column_name, $string_key)>

    my $row = $client->fetch_by_string_from( 'cats', 'name', 'Pixel' );

Similar to C<fetch_by_string> but doesn't require IDs. Instead, it receives
the table and column names and determines the IDs itself.

C<fetch_by_string()> is faster.

=head2 C<fetch_by_int($table_id, $column_id, $numeric_key)>

    my $row = $client->fetch_by_int( 0, 0, 20 );

Like C<fetch_by_string> but deals with keys that are numeric.

For a name-friendly version, see C<fetch_by_int_from()>.

=head2 C<fetch_by_int_from($table_name, $column_name, $numeric_key)>

    my $row = $client->fetch_by_int_from( 'people', 'id', 20 );

Similar to C<fetch_by_int> but doesn't require IDs. Instead, it receives
the table and column names and determines the IDs itself.

C<fetch_by_int()> is faster.

=head2 describe_schema

    my $schema = $client->_load_schema_from_describe();

    # If you connected already, you can run
    my $schema = $client->{'schema'}; # populated automatically

Sends a C<DESCRIBE> action to the server and returns the parsed schema hashref.
Used internally during construction when no explicit schema is provided.

=head1 FUNCTIONS

This is the functional interface to Melian. It's quite a bit faster but is less
pretty and requires a bit of handling. If you're trying to squeeze every bit, this
is for you.

=head2 C<create_connection(%args)>

    my $conn = Melian->create_connection(
        %same_args_as_new_method
    );

Instead of returning a Melian object, it returns a socket connection. You then
feed that socket connection to each function you need.

=head2 C<fetch_raw_with($conn, $table_id, $column_id, $key)>

    my $data = fetch_raw_with( $conn, 1, 1, 'Pixel' );

This is part of the functional interface and requires calling
C<create_connection()> first.

=head2 C<fetch_by_string_with( $conn, $table_id, $column_id, $string_key)>

    my $data = fetch_raw_with( $conn, 1, 1, 'Pixel' );

This is part of the functional interface and requires calling
C<create_connection()> first.

=head2 C<fetch_by_int_with($conn, $table_id, $column_id, $int_key)>

    my $data = fetch_raw_with( $conn, 0, 0, 20 );

This is part of the functional interface and requires calling
C<create_connection()> first.

