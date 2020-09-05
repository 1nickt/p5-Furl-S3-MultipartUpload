package Furl::S3::MultipartUpload;

use strict;
use warnings;
use v5.12;

use Digest::MD5 'md5_base64';
use Furl::S3;
use HTTP::Date;
use Params::Validate qw(:types validate_with validate_pos);
use Type::Params 1.004000 'compile_named';
use Types::Common::String 'NonEmptyStr';
use Types::Standard qw/FileHandle HashRef Int Maybe Str/;

use Moo;
use MooX::TypeTiny;

use namespace::clean;

extends 'Furl::S3';

has multipart_upload_id => (
    is => 'rwp',
);

sub multipart_upload {
    my $self = shift;
    my ($bucket, $key, $content, $headers) = @_;
#    validate_pos( @_,
#        { type => SCALAR, callbacks => { bucket_name => \&Furl::S3::validate_bucket } },
#        { type => SCALAR },
#        { type => HANDLE | SCALAR },
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
        upload_id   => NonEmptyStr, { default => $self->multipart_upload_id },
        content     => NonEmptyStr,
        headers     => HashRef, { default => {} },
    );

    my $p = $check->(@_);

    my $response = $self->request('PUT',
        $p->{bucket},
        $p->{key},
        { part_number => $p->{part_number}, upload_id => $p->{upload_id} },
        { content_length => length($p->{content}), %{ $p->{headers} } },
        { content => $p->{content} },
    );

    my $etag = $response->{headers}{etag};

    return $etag;
}

#-- Complete a multipart upload ---------------------------------------#
#
sub multipart_upload_complete {
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

warn "$xml";

    my $response = $self->request('POST',
        $p->{bucket},
        $p->{key},
        { uploadId => $p->{upload_id} },
        { content_type => 'text/xml', %{ $p->{headers} } },
        { content => $xml },
    );

    return $response;
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
    $str;
}

##
sub request {
    my $self = shift;
    my( $method, $bucket, $key, $params, $headers, $furl_options ) = @_;
    validate_pos( @_, 1, 1,
                  { type => SCALAR | UNDEF, optional => 1 },
                  { type => HASHREF | UNDEF | SCALAR , optional => 1, },
                  { type => HASHREF | UNDEF , optional => 1, },
                  { type => HASHREF | UNDEF , optional => 1, }, );
    $self->clear_error;
    $key ||= '';
    $params ||= +{};
    $headers ||= +{};
    $furl_options ||= +{};

    my %h;
    while (my($key, $val) = each %{$headers}) {
        $key =~ s/_/-/g; # content_type => content-type
        $h{lc($key)} = $val
    }
    if ( !$h{'expires'} && !$h{'date'} ) {
        $h{'date'} = time2str(time);
    }
    my $resource = $self->resource( $bucket, $key );

    # Support for AWS MultipartUpload API
    if ( ! ref $params && $params eq 'uploads' ) {
        $resource .= '?' . $params;
    }
    if ( ref $params eq 'HASH' && exists $params->{uploadId} ) {
        $resource .= '?uploadId=' . $params->{uploadId};
        if ( exists $params->{partNumber} ) {
            $resource .= '&partNumber=' . $params->{partNumber};
        }
    }

    my $string_to_sign =
        $self->string_to_sign( $method, $resource, \%h );
    my $signed_string = $self->sign( $string_to_sign );
    my $auth_header = 'AWS '. $self->aws_access_key_id. ':'. $signed_string;
    $h{'authorization'} = $auth_header;

    my( $host, $path_query ) =
        $self->host_and_path_query( $bucket, $key, $params );
    my %res;
    my @h = %h;

    @res{qw(ver code msg headers body)} = $self->furl->request(
        method => $method,
        scheme => ($self->secure ? 'https' : 'http'),
        host => $host,
        path_query => $path_query,
        headers => \@h,
        %{$furl_options},
    );
    return \%res;
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

This is handy because the parts of an object are uploaded with the normal
C<create_object> call and assembled later -- so you can upload in parallel,
or before your object is completely created.

This module uses L<MCE::Queue> to

=head1 METHODS

=head1 SEE ALSO

L<Furl::S3>
