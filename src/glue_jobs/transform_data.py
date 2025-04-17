import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node album
album_node1732511879035 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-dataeng/staging/albums.csv"], "recurse": True}, transformation_ctx="album_node1732511879035")

# Script generated for node artist
artist_node1732511879579 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-dataeng/staging/artists.csv"], "recurse": True}, transformation_ctx="artist_node1732511879579")

# Script generated for node track
track_node1732511880421 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-dataeng/staging/track.csv"], "recurse": True}, transformation_ctx="track_node1732511880421")

# Script generated for node Join album and artist
Joinalbumandartist_node1732512044934 = Join.apply(frame1=album_node1732511879035, frame2=artist_node1732511879579, keys1=["artist_id"], keys2=["id"], transformation_ctx="Joinalbumandartist_node1732512044934")

# Script generated for node Join track with albumb_artist
Jointrackwithalbumb_artist_node1732512253039 = Join.apply(frame1=track_node1732511880421, frame2=Joinalbumandartist_node1732512044934, keys1=["track_id"], keys2=["track_id"], transformation_ctx="Jointrackwithalbumb_artist_node1732512253039")

# Script generated for node Drop Fields
DropFields_node1732512360408 = DropFields.apply(frame=Jointrackwithalbumb_artist_node1732512253039, paths=["`.track_id`", "id"], transformation_ctx="DropFields_node1732512360408")

# Script generated for node Destination
Destination_node1732512434334 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1732512360408, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-spotify-dataeng/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1732512434334")

job.commit()