package DB::DatabaseHandler;

=head1 NAME

DB::DatabaseHandler

=head1 SYNOPSIS

Class for handling connection to the database.

=head1 DESCRIPTION

DB::DatabaseHandler provides an object oriented mechanism for handling connection to the database.

=cut

use strict;
use DBI;

use Env::EnvHandler;
use Service::ServiceError;
use Service::ServiceConfig;
use Service::ErrorHandler;

use Data::Dumper;

# Using parameters validation module with on_fail error handler calling. (!) - cool thing
use Params::Validate qw( validate_pos validation_options HASHREF SCALARREF );
validation_options( on_fail => sub { Service::ErrorHandler::error_handler( Service::ServiceError::VALIDATION_FAILED, $_[0] ); } );
###

=head1 Object Methods

=head2 B<new()>

    The constructor.

=cut

# my $shared_db;

my $env = Env::EnvHandler->new();

sub new {
#    return $shared_db if $shared_db;

    my ( $class_or_obj, $db_info ) = validate_pos( @_, 1, { type => HASHREF } );
    my $class = ref( $class_or_obj ) || $class_or_obj;

    my $self = {
        '_dbh'                      => undef,
        'data_ref'                  => undef,
        'amount_rows_current'       => undef,
        'amount_rows_without_limit' => undef,
        'pages_amount'              => undef,
        '_db_info'                  => $db_info,
        'debug'                     => undef,
    };

    bless $self, $class;
#    $shared_db = $self;

    return $self;
}

=head2 B<connect()>

    this method will connecto to the database, depending on Service::ServiceConfig parameters.
    Returns Service::ServiceError::OK or udnef

=cut

my $sql_set_names = 'SET NAMES utf8';

sub connect {
    return Service::ServiceError::OK if ( defined( $_[0]->{'_dbh'} ) );

    my ( $self ) = @_;

    $self->{'_dbh'} = DBI->connect_cached(
        'dbi:mysql:dbname=' . $self->{'_db_info'}->{'db_name'} .
        ';host=' . $self->{'_db_info'}->{'db_host'} .
        ';mysql_connect_timeout=' . $self->{'_db_info'}->{'mysql_connect_timeout'} .
        ';port=' . $self->{'_db_info'}->{'db_port'},
        $self->{'_db_info'}->{'db_user'},
        $self->{'_db_info'}->{'db_password'},
        {
            'RaiseError' => 0,
            'PrintError' => 0,
            'AutoCommit' => 1,
            'mysql_auto_reconnect' => 1
        }
    );

    return Service::ErrorHandler::error_handler( Service::ServiceError::DB_CONNECTION_ERROR, $DBI::errstr ) unless $self->{'_dbh'};

    $self->_execute_query( \$sql_set_names );

    return Service::ServiceError::OK;
};

=head2 B<select_from_db()>

    This method will select records from the database.
    Required parameters:

=over 8

=item \$sql - the ref to an SQL string;

=item \@parameters - the ref to an array of the bind parameters; [optional]

=item $type - the type of the selecting method [optional] :
    1 - selectrow_hashref;
    else - selectall_arrayref.

=back

    Returns:
    - reference to an array containing a reference to the hash for each row of data fetched;
    - reference to the hash containing one row of data fetched.

=cut

sub select_from_db (\$\$\@$) {
    my ( $self, $sql, $parameters, $type ) = validate_pos( @_, 1, { type => SCALARREF }, 0, 0 );

    $self->{'data_ref'} = undef; # clearing previous set
    $self->{'amount_rows_current'} = 0;

    $self->connect() unless defined( $self->{'_dbh'} );
    return Service::ErrorHandler::error_handler( Service::ServiceError::DB_CONNECTION_NOT_DEFINED ) unless defined( $self->{'_dbh'} );

    $env->log_it( "SQL query:\n" . $$sql . ( $parameters ? "\nParameters:\n" . join( ', ', @{ $parameters } ) : '' ) ) if $self->{'debug'};

    if ( $type ) {
        $self->{'data_ref'} = $self->{'_dbh'}->selectrow_hashref( ${$sql}, undef, @{ $parameters } );
        if ( ( keys %{ $self->{'data_ref'} } ) > 0 ) { $self->{'amount_rows_current'} = 1; }
    } else {
        $self->{'data_ref'} = $self->{'_dbh'}->selectall_arrayref( ${$sql}, { Slice => {} }, @{ $parameters } );
        $self->{'amount_rows_current'} = scalar( @{ $self->{'data_ref'} } ) if $self->{'data_ref'};
    }
    if ( $self->{'_dbh'}->errstr ) {

        return undef if Service::ServiceConfig::ERROR_ACTION eq Service::ServiceParams::ERROR_NONE;

        return Service::ErrorHandler::error_handler( Service::ServiceError::DB_QUERY_ERROR, $self->{'_dbh'}->errstr . ' ( ' . ${$sql} . ' )' );
    }

    return $self->{'data_ref'};
}

sub select_row_from_db (\$\$\@) {
    my ( $self, $sql, $parameters ) = @_;
    return $self->select_from_db( $sql, $parameters, 1 );
}

sub select_last_insert_id {
    my ( $self ) = @_;
    my $result = $self->select_from_db( \'SELECT LAST_INSERT_ID() AS `last_insert_id`', undef, 1 );
    return $result->{'last_insert_id'};
}

sub _execute_query (\$\$\@) {
    my ( $self, $sql, $parameters ) = validate_pos( @_, 1, { type => SCALARREF }, 0 );

    $self->connect() unless defined( $self->{'_dbh'} );
    return Service::ErrorHandler::error_handler( Service::ServiceError::DB_CONNECTION_NOT_DEFINED ) unless defined( $self->{'_dbh'} );

    $env->log_it( "SQL query:\n" . $$sql . ( $parameters ? "\nParameters:\n" . join( ', ', @{ $parameters } ) : '' ) ) if $self->{'debug'};

    my $sth = $self->{'_dbh'}->prepare_cached( $$sql, {} );
    if ( $sth->execute( @$parameters ) ) {
        $sth->finish();
    } else {
        return Service::ErrorHandler::error_handler( Service::ServiceError::DB_QUERY_ERROR, $self->{'_dbh'}->errstr . ' ( ' . ${$sql} . ' )' );
    };

    return Service::ServiceError::OK;
}

=head2 B<insert_into_db()>

    This method will insert records into the database.
    Required parameters:

=over 8

=item \$ - the ref to an SQL string;

=item \$ - the ref to an array of the bind parameters.

=back

    Returns Service::ServiceError::OK or undef

=cut

sub insert_into_db (\$\$\@) {
    return &_execute_query;
}

=head2 B<delete_from_db()>

    This method will delete records from the database.
    Required parameters:

=over 8

=item \$ - the ref to an SQL string;

=item \$ - the ref to an array of the bind parameters.

=back

    Returns Service::ServiceError::OK or undef

=cut

sub delete_from_db (\$\$\@) {
    return &_execute_query;
}

=head2 B<update_db()>

    This method will update records in the database.
    Required parameters:

=over 8

=item \$ - the ref to an SQL string;

=item \@ - the ref to an array of the bind parameters [optional].

=back

    Returns Service::ServiceError::OK or undef

=cut

sub update_db (\$\$\@) {
    return &_execute_query;
}

sub calculate_pages_amount {
    my ( $self, $records ) = validate_pos( @_, 1, 0 );

    $records = $env->{'web_table_records_amount'} if !$records;

    $self->{'data_ref'} = undef; # clearing previous set
    $self->{'pages_amount'} = 0;

    $self->connect() unless defined( $self->{'_dbh'} );
    return Service::ErrorHandler::error_handler( Service::ServiceError::DB_CONNECTION_NOT_DEFINED ) unless defined( $self->{'_dbh'} );

    my $amount = $self->select_row_from_db( \'select FOUND_ROWS() as amount' );
    return Service::ErrorHandler::error_handler( Service::ServiceError::NO_DATA ) unless defined( $amount->{'amount'} );

    $self->{'amount_rows_without_limit'} = $amount->{'amount'};
    $self->{'pages_amount'} = $self->{'amount_rows_without_limit'} / $records;
    if ( $self->{'pages_amount'} > int( $self->{'pages_amount'} ) ) { $self->{'pages_amount'} = int( $self->{'pages_amount'} ) + 1; };

    return $self->{'pages_amount'};
}

sub disconnect {
    my ( $self ) = validate_pos( @_, 1 );

    return Service::ErrorHandler::error_handler( Service::ServiceError::DB_CONNECTION_NOT_DEFINED ) unless defined( $self->{'_dbh'} );

    $self->{'_dbh'}->disconnect;
    undef( $self->{'_dbh'} );

    return Service::ServiceError::OK;
}

sub DESTROY {
    my ( $self ) = @_;

#     undef( $self->{'_dbh'} );
#     undef( $self->{'data_ref'} );
#     undef( $self->{'amount_rows_current'} );
#     undef( $self->{'amount_rows_without_limit'} );
#     undef( $self->{'pages_amount'} );
#     undef( $self->{'_db_info'} );
#     undef( $shared_db );

    1;
}

1;
