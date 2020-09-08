package Furl::S3::MultipartUpload;

use strict;
use warnings;
use v5.12;

use Digest::MD5 'md5_base64';
use Furl::S3;
use HTTP::Date;
use Params::Validate qw(:types validate_with validate_pos);
use Type::Params 1.004000 'compile', 'compile_named';
use Types::Common::String 'NonEmptyStr';
use Types::Standard 'Enum', 'HashRef', 'Int', 'Str';

use Moo;
use MooX::TypeTiny;

use namespace::clean;

extends 'Furl::S3';

has multipart_upload_id => (
    is  => 'rwp',
    isa => NonEmptyStr,
);

sub multipart_upload {
    my $self = shift;
    my ($bucket, $key, $file, $headers) = @_;
#    validate_pos( @_,
#        { type => SCALAR, callbacks => { bucket_name => \&Furl::S3::validate_bucket } },
#        { type => SCALAR },
#        { type => SCALAR },
#        { type => HASHREF, optional => 1 },
#    );

    my $response = $self->create_multipart_upload( $bucket, $key, $headers);
}

#-- Initialize a multipart upload -------------------------------------#
#
sub multipart_create {
    my $self = shift;

    state $check = compile_named(
        bucket  => Str->where( sub{ die 'invalid bucket name' unless Furl::S3::validate_bucket($_) } ),
        key     => NonEmptyStr,
        headers => HashRef, { default => {} },
    );

    my $p = $check->(@_);

    my $response = $self->request('POST',
        $p->{bucket},
        $p->{key},
        'uploads',
        { content_type => 'multipart/form-data', %{ $p->{headers} } },
    );

    my $xpc = $self->_create_xpc( $response->{body} );
    my $id = $xpc->findvalue('/s3:InitiateMultipartUploadResult/s3:UploadId');
    $self->_set_multipart_upload_id($id);

    return $id;
}

#-- Upload a part ----------------------------------------------------#
#
sub multipart_upload_part {
    my $self = shift;

    state $check = compile_named(
        bucket      => Str->where( sub { die 'invalid bucket name' unless Furl::S3::validate_bucket($_) } ),
        key         => NonEmptyStr,
        part_number => Int->where( sub { die 'invalid part number' unless $_ >= 1 && $_ <= 10_000 }),
        upload_id   => NonEmptyStr,
        content     => NonEmptyStr,
        headers     => HashRef, { default => {} },
    );

    my $p = $check->(@_);

    my $response = $self->request('PUT',
        $p->{bucket},
        $p->{key},
        { uploadId => $p->{upload_id}, partNumber => $p->{part_number} },
        { content_length => length($p->{content}), %{ $p->{headers} } },
        { content => $p->{content} },
    );

    return $self->error( $response ) if ! Furl::S3::_http_is_success( $response->{code} );
    return $self->_normalize_response( $response )->{etag};
}

#-- Complete a multipart upload ---------------------------------------#
#
sub multipart_complete {
    my $self = shift;

    state $check = compile_named(
        bucket    => Str->where( sub { die 'invalid bucket name' unless Furl::S3::validate_bucket($_) } ),
        key       => Str,
        upload_id => Str, { default => $self->multipart_upload_id },
        parts     => HashRef,
        headers   => HashRef, { default => {} },
    );

    my $p = $check->(@_);

    my $xml = '<CompleteMultipartUpload>';
    $xml .= sprintf('<Part><PartNumber>%s</PartNumber><ETag>%s</ETag></Part>', $_, $p->{parts}{$_})
        for sort { $a <=> $b } keys %{ $p->{parts} };
    $xml .= '</CompleteMultipartUpload>';

    my $response = $self->request('POST',
        $p->{bucket},
        $p->{key},
        { uploadId => $p->{upload_id} },
        { content_type => 'text/xml', %{ $p->{headers} } },
        { content => $xml },
    );

    return $self->error( $response ) if ! Furl::S3::_http_is_success( $response->{code} );
    return 1;
}

#-- List parts in a multipart upload ----------------------------------#
#
sub multipart_upload_list_parts {
    my $self = shift;

    state $check = compile_named(
        bucket      => Str->where( sub { die 'invalid bucket name' unless Furl::S3::validate_bucket($_) } ),
        key         => NonEmptyStr,
        upload_id   => NonEmptyStr, { default => $self->multipart_upload_id },
        headers     => HashRef, { default => {} },
    );

    my $p = $check->(@_);

    my $response = $self->request('GET',
        $p->{bucket},
        $p->{key},
        { uploadId => $p->{upload_id} },
        $p->{headers},
    );

return $response;

    my $etag = $response->{headers}{etag};

    return $etag;
}

#-- The following two routines are taken from Furl::S3 and extended --#
#-- to add support for the MulipartUpload API.                      --#
##
sub string_to_sign {
    my( $self, $method, $resource, $headers ) = @_;
    $headers ||= {};
    my %headers_to_sign;
    while (my($k, $v) = each %{$headers}) {
        my $key = lc $k;
        if ( $key =~ /^(content-md5|content-type|date|expires)$/ or
                 $key =~ /^x-amz-/ ) {
            $headers_to_sign{$key} = &Furl::S3::_trim($v);
        }
    }
    my $str = "$method\n";
    $str .= $headers_to_sign{'content-md5'} || '';
    $str .= "\n";
    $str .= $headers_to_sign{'content-type'} || '';
    $str .= "\n";
    $str .= $headers_to_sign{'expires'} || $headers_to_sign{'date'} || '';
    $str .= "\n";
    for my $key( sort grep { /^x-amz-/ } keys %headers_to_sign ) {
        $str .= "$key:$headers_to_sign{$key}\n";
    }
    my( $path, $query ) = split /\?/, $resource;
    # sub-resource.
    if ( $query && ( $query =~ m{^(acl|policy|location|versions|uploads)$} ||
                     $query =~ m{uploadId=}) ) {
        $str .= $resource;
    }
    else {
        $str .= $path;
    }

    warn "STR $str";
    $str;
}

##
sub request {
    my $self = shift;

    state $check = compile(
        Enum[qw/DELETE HEAD GET POST PUT/],
        Str->where( sub { die 'invalid bucket name' unless Furl::S3::validate_bucket($_) } ),
        NonEmptyStr,
        NonEmptyStr | HashRef, { default => {} },
        HashRef, { default => {} },
        HashRef, { default => {} },
    );

    my $p = $check->(@_);

    $self->clear_error;
    $p->{params} ||= +{};

    my %h;
    while (my($key, $val) = each %{$p->{headers}}) {
        $key =~ s/_/-/g; # content_type => content-type
        $h{lc($key)} = $val
    }
    if ( !$h{'expires'} && !$h{'date'} ) {
        $h{'date'} = time2str(time);
    }
    my $resource = $self->resource( $p->{bucket}, $p->{key} );

    ## Support for AWS MultipartUpload API
    # multipart_upload_create
    $resource .= '?' . $p->{params} if ! ref $p->{params} && $p->{params} eq 'uploads';

    if ( ref $p->{params} eq 'HASH' && exists $p->{params}{uploadId} ) {
         # multipart_upload_part
         if ( exists $p->{params}{partNumber} ) {
            $resource .= sprintf('?partNumber=%s&uploadId=%s', $p->{params}{partNumber}, $p->{params}{uploadId});
        }
        # multipart_upload_complete
        else {
            $resource .= sprintf('?uploadId=%s', $p->{params}{uploadId});
        }
    }
    ##

    my $string_to_sign = $self->string_to_sign( $p->{method}, $resource, \%h );
    my $signed_string = $self->sign( $string_to_sign );
    my $auth_header = 'AWS '. $self->aws_access_key_id. ':'. $signed_string;
    $h{'authorization'} = $auth_header;

    my( $host, $path_query ) = $self->host_and_path_query( $p->{bucket, $p->{key}, $p->{params} );
    my %response;
    my @h = %h;

    @response{qw(ver code msg headers body)} = $self->furl->request(
        method     => $p->{method},
        scheme     => ($self->secure ? 'https' : 'http'),
        host       => $host,
        path_query => $path_query,
        headers    => \@h,
        %{ $p->{furl_options} },
    );
    return \%response;
}

##
1; # return true

__END__

=pod

=encoding utf8

=head1 NAME

Furl::S3::MultipartUpload

=head1 DESCRIPTION

Furl::S3 extension supporting AWS S3 Multipart Uploads

See: L<https://docs.aws.amazon.com/AmazonS3/latest/dev/uploadobjusingmpu.html>

Amazon recommends not using a single PUT request to create objects
larger than ~ 100Mb. L<Furl> does not support chunked requests (see why at
L<https://metacpan.org/pod/Furl::HTTP#FAQ>). However, Amazon provides the
"Multipart Upload API".

This is handy because the parts of an object are uploaded with individual
C<PUT> calls and assembled later -- so you can upload in parallel, or
intermittently over time, or starting before your object is completely
created.

This module uses L<MCE::Queue> to

=head1 METHODS

=over

=item B<multipart_upload (:$bucket :Str, :$key :Str, :$file :Str, [:$headers :Href])>

Convenience method wrapping a C<multipart_create()> call, multiple
C<multipart_upload_part> calls, and a C<multipart_complete()> call, to
upload a large file ion one operation.

=item B<multipart_create (:$bucket :Str, :$key :Str, [:$headers :Href])>

Initializes a multipart upload to the specified key ion the specified
bucket. Returns an upload ID that must be stored and used for later
part upload and upload completion requests. For convenience the ID is
stored in the instance's C<multipart_upload_id> attribute.

=item B<multipart_upload_part (:$bucket :Str, :$key :$Str, :$part_number :$Int, :$upload_id :Str, :$content :Str, [:$headers :Href])>

Uploads a part of a file. Minimum size (except for the last part) is 5MB.
Returns an ETag, which must be stored along with the part number provided
for later use with the upload completion request.

=item B<multipart_complete (:$bucket :Str, :$key :Str, :$upload_id :Str, :$parts :Href, [:$headers :Href])>

Completes a multipart upload. Returns true on success. The C<parts>
parameter must be a hashref of ETags keyed by part numbers corresponding
to the parts uploaded.

=back

=head1 SEE ALSO

L<Furl::S3>
