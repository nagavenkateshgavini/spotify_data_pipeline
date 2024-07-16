import json
import os

import boto3
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

client_id = os.environ.get("client_id")
secret_key = os.environ.get("secret_key")
auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=secret_key)
sp = spotipy.Spotify(auth_manager=auth_manager)


def lambda_handler(event, context):
    playlist_link = "https://open.spotify.com/playlist/5ABHKGoOzxkaa28ttQV9sE"

    playlist_id = playlist_link.split("/")[-1]
    spotify_data = sp.playlist_items(playlist_id)

    s3_client = boto3.client("s3")

    file_name = f"spotify_raw_{str(datetime.now())}.json"
    s3_client.put_object(
        Bucket="spotify-etl-pipeline-neu",
        Key=f"raw_data/unprocessed/{file_name}",
        Body=json.dumps(spotify_data)
    )

    glue = boto3.client('glue')
    glue_job_name = "spotify_tranformation_job"

    try:
        run_id = glue.start_job_run(JobName=glue_job_name)
        status = glue.get_job_run(JobName=glue_job_name, RunId=run_id['JobRunId'])
        print("Job Status: ", status.get('JobRun', {}).get('JobRunState'))
    except Exception as e:
        print(e)
