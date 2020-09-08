use strict;
use warnings;
use Test::Most 'die';

use Furl::S3::MultipartUpload;

unless ( $ENV{TEST_AWS_ACCESS_KEY_ID} && $ENV{TEST_AWS_SECRET_ACCESS_KEY} ) {
    plan skip_all => 'online tests are skipped';
}

my $client = Furl::S3::MultipartUpload->new(
    aws_access_key_id     => $ENV{TEST_AWS_ACCESS_KEY_ID},
    aws_secret_access_key => $ENV{TEST_AWS_SECRET_ACCESS_KEY},
    secure                => 1,
);

my $bucket = $ENV{TEST_AWS_S3_BUCKET} || lc('test-'. $ENV{TEST_AWS_ACCESS_KEY_ID}. '-'. time);

##
subtest 'Create a multipart upload' => sub {
    ok(
        my $upload_id = $client->multipart_create(
            bucket => $bucket,
            key    => 'foo',
        ),
        'create multipart upload',
    );

    note "Upload ID: $upload_id";

    isnt( length $upload_id, 0, 'string has length' );

    is( $upload_id, $client->multipart_upload_id, 'ID is set in client object' );
};

my %part_list;

##
subtest 'Upload a file in parts' => sub {
    my $chunk_size = 5_242_880;
    my $i = 1;
    open my $fh, '<', '/Users/1nickt/msft-2020-07-18.csv' or die $!;

    while (read $fh, my $buffer, $chunk_size) {
        ok(
            my $etag = $client->multipart_upload_part(
                bucket      => $bucket,
                key         => 'foo',
                upload_id   => $client->multipart_upload_id,
                part_number => $i,
                content     => $buffer,
            ),
            'upload multipart part ' . $i,
        );

        note "ETag: $etag";
        $part_list{$i} = $etag;
        $i++;
    }
};

##
subtest 'Complete multipart upload' => sub {
    ok(
        my $res = $client->multipart_complete(
            bucket      => $bucket,
            key         => 'foo',
            upload_id   => $client->multipart_upload_id,
            parts       => \%part_list,
        ),
        'complete multipart upload',
    );

    explain $res;
};

done_testing;
