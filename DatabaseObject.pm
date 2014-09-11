package DB::DatabaseObject;

=head1 NAME

DB::DatabaseObject - base class for getting access to the DB tables and their fields.

=cut

use strict;

use Service::ServiceConfig;
use Service::ServiceError;
use Env::EnvHandler;
use Service::ErrorHandler;
use DB::DatabaseHandler;

use Data::Dumper;

# Using parameters validation module with on_fail error handler {
use Params::Validate qw( validate_pos validation_options HASHREF ARRAYREF );
validation_options( on_fail => sub { Service::ErrorHandler::error_handler( Service::ServiceError::VALIDATION_FAILED, $_[0] ); } );
# }

our $AUTOLOAD;

=over 4

=item B<new()>

Constructor.

=back

=cut

sub new {
    my ( $class_or_obj, $dbh, $table, $params ) = validate_pos( @_, 1, 1, 1, 0 );

    my $class = ref( $class_or_obj ) || $class_or_obj;

    $params                     ||= {};
    $params->{'id_field'}       ||= 'id';
    $params->{'select_fields'}  ||= '*';

    my $self = {
        'table'                     => $table,
        'fields'                    => undef,
        'hash'                      => undef,
        '_keys'                     => undef,
        '_loaded'                   => undef,
        '_updated'                  => undef,
        '_params'                   => $params,
        '_dbh'                      => $dbh,
    };

    bless $self, $class;
    return $self;
};

sub AUTOLOAD {
    my ( $self, $value ) = validate_pos( @_, { type => HASHREF }, 0 );

    my ( $field ) = $AUTOLOAD =~ m!^.*:(.*)$!;

# if you want to make error exception while accessing not existing field - uncoment next code line
# ( but then you will not be able to store new field for save() method, if load() method didn't return any data ).

#    return Service::ErrorHandler::error_handler( Service::ServiceError::WRONG_CODE ) unless ( exists ( $self->{'fields'}->{$field} ) );

    if ( defined( $value ) ) {
       $self->{'fields'}->{$field} = $value;
       $self->{'_updated'}->{$field}++;
    } else {
        return $self->{'fields'}->{$field};
    }
    return Service::ServiceError::OK;
};

=head2 B<load( { 'id' => 4 } )>

    Returns amount of fileds loaded for passed parameters.
    Required parameters:

=over 8

=item $params - the ref to the hash, that contain pairs key-value, where key is the field name and value is the value, that will be included into WHERE part of the sql.
    (!) important - this pairs key-value will be used in method save for storing information about loaded record ( this keys-values can affect several records in save() method (!) ).

=back

    Returns:
    - reference to an array containing a reference to the hash for each row of data fetched;
    - reference to the hash containing one row of data fetched.

=cut

sub load {
    my ( $self, $params ) = validate_pos( @_, { type => HASHREF }, { type => HASHREF } );

    my $sql = 'SELECT ' . $self->{'_params'}->{'select_fields'} . ' FROM `' . $self->{'table'} . '` WHERE ';

    my ( @fields, @binds_parameters, $real_name, $condition );

    $self->{'_keys'}    = undef;
    $self->{'fields'}   = undef;
    $self->{'hash'}     = undef;

    foreach my $field_name ( keys %{ $params } ) {
        if ( defined( $params->{ $field_name } ) ) {

            $real_name = $field_name;
            $condition = '=';

            if ( index( $real_name, '!' ) == 0 ) {
                $real_name =~ s/^!//;
                $condition = '!=';
            }

            $self->{'_keys'}->{ $field_name } = $params->{ $field_name };

            if ( ref( $params->{ $field_name } ) && $params->{ $field_name }->{'function'} ) {

                $fields[ @fields ]                        = "`$real_name` $condition " . $params->{ $field_name }->{'function'};
                $binds_parameters[ @binds_parameters ]  = $params->{ $field_name }->{'parameter'} if defined( $params->{ $field_name }->{'parameter'} );

            } else {

                $fields[@fields]                        = "`$real_name` $condition ?";
                $binds_parameters[ @binds_parameters ]  = $params->{ $field_name };
            }

        }
    };

    $sql .= join( ' AND ', @fields ) . ' LIMIT 1';

    my $result = $self->{'_dbh'}->select_row_from_db( \$sql, \@binds_parameters );
    return Service::ErrorHandler::error_handler( Service::ServiceError::NO_DATA ) if !$result;

    my $fields_loaded = 0;

    foreach my $field ( keys %{ $result } ) {
        $self->{'fields'}->{ $field } = $result->{ $field };
        $fields_loaded++;
    };

    if ( $fields_loaded ) {
        $self->{'hash'} = $result;
    } else {
        %{ $self->{'fields'} } = %{ $self->{'_keys'} };
    }

    $self->{'_loaded'} = $fields_loaded;

    return $fields_loaded;
};

sub set_field {
    my ( $self, $params ) = validate_pos( @_, { type => HASHREF }, { type => HASHREF } );

    my $updated = 0;

    foreach my $key ( keys %{ $params } ) {
        $self->{'fields'}->{ $key } = $params->{ $key };
        $self->{'_updated'}->{ $key }++;
        $updated++;
    };

    return $updated;
};

sub get_field {
    my ( $self, $fields ) = validate_pos( @_, { type => HASHREF }, { type => ARRAYREF } );

    my @values;

    foreach my $field ( @{ $fields } ) {
        $values[@values] = $self->{'fields'}->{$field};
    };

    return $values[0] if @values == 1;
    return @values;
};

sub _insert {
    my ( $self, $params ) = validate_pos( @_, { type => HASHREF }, 0 );

    my $sql = qq~INSERT INTO `$self->{'table'}` ( ~;

    my ( @fields, @binds_parameters, $bind );

    if ( ref( $params ) ) {
        foreach my $key ( keys %{ $params } ) {
            if ( defined( $params->{$key} ) ) {
                $fields[@fields] = "`$key`";
                $binds_parameters[@binds_parameters] = $params->{$key};
                $bind = $bind ? $bind . ', ?' : '?';
                $self->{'fields'}->{$key} = $params->{$key};
            }
        };
    } else {
        foreach my $key ( keys %{ $self->{'fields'} } ) {
            if ( defined( $self->{'fields'}->{ $key } ) ) {

                $fields[@fields] = "`$key`";

                if ( ref( $self->{'fields'}->{ $key } ) && $self->{'fields'}->{ $key }->{'function'} ) {

                    $bind = $bind ? $bind . ', ' . $self->{'fields'}->{ $key }->{'function'} : $self->{'fields'}->{ $key }->{'function'};
                    $binds_parameters[ @binds_parameters ] = $self->{'fields'}->{ $key }->{'parameter'} if defined( $self->{'fields'}->{ $key }->{'parameter'} );

                } else {

                    $bind = $bind ? $bind . ', ?' : '?';
                    $binds_parameters[ @binds_parameters ] = $self->{'fields'}->{$key};

                }

            }
        };
    }

    $sql .= join( ', ', @fields ) . " ) VALUES ( $bind )";

    return Service::ErrorHandler::error_handler( Service::ServiceError::DB_QUERY_ERROR ) unless Service::ErrorHandler::error_handler( $self->{'_dbh'}->insert_into_db( \$sql, \@binds_parameters ) ) == Service::ServiceError::OK;

    $self->{'_keys'} = { $self->{'_params'}->{'id_field'} => $self->{'_dbh'}->select_last_insert_id() };

#     if ( !$self->{'_keys'} ) {
#         %{$self->{'_keys'}} = %{$self->{'fields'}};
#     }

    return $self->load( $self->{'_keys'} );
};

sub save {
    my ( $self, $params ) = validate_pos( @_, { type => HASHREF }, 0 );

    if ( !$self->{'fields'} && !ref( $params ) ) { return Service::ErrorHandler::error_handler( Service::ServiceError::NO_DATA ); }

    if ( !$self->{'_loaded'} ) { return $self->_insert( $params ); }

    my $sql = qq~UPDATE `$self->{'table'}` SET ~;

    my ( @fields, @binds_parameters, $real_name, $condition );

    if ( $params ) {
        foreach my $key ( keys %{ $params } ) {
            if ( defined( $params->{$key} ) ) {
                $fields[@fields] = "`$key` = ?";
                $binds_parameters[@binds_parameters] = $params->{$key};
            }
        };
    } else {
        foreach my $key ( keys %{ $self->{'fields'} } ) {
            if ( $self->{'_updated'}->{$key} && defined( $self->{'fields'}->{$key} ) ) {

                if ( ref( $self->{'fields'}->{ $key } ) && $self->{'fields'}->{ $key }->{'function'} ) {

                    $fields[@fields] = "`$key` = " . $self->{'fields'}->{ $key }->{'function'};
                    $binds_parameters[ @binds_parameters ] = $self->{'fields'}->{ $key }->{'parameter'} if defined( $self->{'fields'}->{ $key }->{'parameter'} );

                } else {

                    $fields[ @fields ] = "`$key` = ?";
                    $binds_parameters[ @binds_parameters ] = $self->{'fields'}->{$key};

                }

                $self->{'_updated'}->{$key} = 0;

            }
        };
    }

    return Service::ServiceError::OK if !@fields;

    $sql .= join( ', ', @fields ) . " WHERE ";

    my @fields_where;

    foreach my $key ( keys %{ $self->{'_keys'} } ) {

        $real_name = $key;
        $condition = '=';

        if ( index( $real_name, '!' ) == 0 ) {
            $real_name =~ s/^!//;
            $condition = '!=';
        }


        $fields_where[@fields_where] = "`$real_name` $condition ?";
        $binds_parameters[@binds_parameters] = $self->{'_keys'}->{$key};

    };

    $sql .= join( ' AND ', @fields_where );

    return Service::ErrorHandler::error_handler( $self->{'_dbh'}->update_db( \$sql, \@binds_parameters ) );

#     return $self->load( $params || $self->{'fields'} );
};

sub delete {
    my ( $self, $params ) = validate_pos( @_, { type => HASHREF }, 0 );

    if ( !$self->{'_loaded'} && !$params ) { return Service::ErrorHandler::error_handler( Service::ServiceError::NO_DATA ); }

    my $sql = qq~DELETE FROM `$self->{'table'}` WHERE ~;

    my ( @fields, @binds_parameters );

    my @fields_where;

    if ( $params ) {
        foreach my $key ( keys %{ $params } ) {
            if ( defined( $params->{$key} ) ) {
                $fields_where[@fields_where] = "`$key` = ?";
                $binds_parameters[@binds_parameters] = $params->{$key};
            }
        };
    } else {
        foreach my $key ( keys %{ $self->{'_keys'} } ) {
            if ( defined( $self->{'_keys'}->{$key} ) ) {
                $fields_where[@fields_where] = "`$key` = ?";
                $binds_parameters[@binds_parameters] = $self->{'_keys'}->{$key};
            }
        };
    }
    $sql .= join( ' AND ', @fields_where );

    return Service::ErrorHandler::error_handler( $self->{'_dbh'}->delete_from_db( \$sql, \@binds_parameters ) );
};

sub DESTROY {
    my ( $self ) = @_;
    undef( $self->{'table'} );
    undef( $self->{'fields'} );
    undef( $self->{'_keys'} );
    undef( $self->{'_loaded'} );
    undef( $self->{'_updated'} );
    undef( $self->{'_dbh'} );
    1;
};

1;
