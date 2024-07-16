CREATE DATABASE spotify_db;

-- Make a connection to S3

create or replace storage integration s3_init
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = "arn:aws:iam::857423392342:role/spotify-snowflake-role"
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-pipeline-neu')
    COMMENT = 'Creating connection to S3';

DESC integration s3_init;

CREATE OR REPLACE file format csv_fileformat
    type = csv
    skip_header = 1
    null_if = ('NULL', 'null')
    empty_field_as_null = TRUE;

CREATE OR REPLACE stage spotify_stage
    URL = 's3://spotify-etl-pipeline-neu/transformed_data/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = csv_fileformat;

LIST @spotify_stage;

-- Create tables

CREATE OR REPLACE TABLE tbl_album (
    album_id STRING,
    album_name STRING,
    release_date DATE,
    total_tracks INT,
    url STRING
);


CREATE OR REPLACE TABLE tbl_artists (
    artist_id STRING,
    name STRING,
    url STRING
);


CREATE OR REPLACE TABLE tbl_songs (
    song_id STRING,
    song_name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added STRING,
    album_id STRING,
    artist_id STRING
);

SELECT * from tbl_songs;

-- Insert initial data into tables

COPY INTO tbl_songs
FROM @spotify_stage/songs/songs_transformed_2024-07-15/run-1721057241434-part-r-00009;

COPY INTO tbl_artists
FROM @spotify_stage/artist/artist_transformed_2024-07-15/run-1721056729741-part-r-00004;

COPY INTO tbl_album
FROM @spotify_stage/album/album_transformed_2024-07-15/run-1721056729462-part-r-00001;

SELECT * from tbl_album;

-- create snowpipe

CREATE OR REPLACE SCHEMA pipe;

CREATE OR REPLACE pipe spotify_db.pipe.tbl_songs_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.public.tbl_songs
FROM @spotify_db.public.spotify_stage/songs;


CREATE OR REPLACE pipe spotify_db.pipe.tbl_artist_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.public.tbl_artists
FROM @spotify_db.public.spotify_stage/artist/;


CREATE OR REPLACE pipe spotify_db.pipe.tbl_album_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.public.tbl_album
FROM @spotify_db.public.spotify_stage/album/;


-- DESC is for creating SQS notification
-- SELECT is to debug the event notifications

-- DESC pipe pipe.tbl_songs_pipe;

-- SELECT COUNT(*) FROM tbl_songs;


-- SELECT SYSTEM$PIPE_STATUS('pipe.tbl_songs_pipe');


-- DESC pipe pipe.tbl_artist_pipe;

-- DESC pipe pipe.tbl_album_pipe;

SELECT SYSTEM$PIPE_STATUS('pipe.tbl_album_pipe');

SELECT COUNT(*) FROM tbl_songs;
