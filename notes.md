df.write \
.format(“com.databricks.spark.redshift”) \
.option(“url”, “jdbc:url/dbname?user=username&password=somepassword”) \
.option(“dbtable”, “schema.tablename”) \
.option(“tempdir”, “s3n://sparkpoc/temp/”) \
.mode(“error”) \
.save()


/usr/bin/spark-shell --packages databricks:spark-deep-learning:1.5.0-spark2.4-s_2.11
--packages org.apache.hadoop:hadoop-aws:2.7.3

TODO 
1. build super simple test spark script that load images from S3 in to a Spark RDD
    - Import sparkdl to see if it is there. 
    - Not there, maybe sparkdl is not the magic. 
    - https://docs.databricks.com/applications/deep-learning/inference/resnet-model-inference-tensorflow.html
    - tfrecords?  Is there an alternative?  Could this be the thing that solves the single classification? 

2. load spark script to emr
    - done.

3. run spark-submit with --package to see if it loads. 
    - This worked.  sort of. Installed databricks packages, but could not import sparkdl -missing keras.backend
    - Perhaps this isn't needed.   

NEXT TODO 
1. look up sparkdl tutorials and try the features. 
    2. Can I load my own graph.pb for transfer learning? 
    3. Can I get EMR to df.write to Redshift? 

2. After running --packages with SparkdL on the master node, open jupyter to refine image script. 

3. refactor spark_image.py. Run this script in ssh to see if it works. 

NEXT TODO 
1. Load the script into S3. 

2. Add bootstrap command to AWS S3 CP 'filename' on to cluster at launch. 

3. test Airflow 



So far need to install: 

pandas - had to `sudo python3 -m pip install pandas keras pillow`
git (for download)
contextlib2

try spark-submit without path

### adding sparkdl to emr notebook
in emr notebook: 
sc.list_packages()
sc.install_pypi_package("keras")
sc.install_pypi_package("tensorframes")
sc.install_pypi_package("pillow")

IMAGES_PATH = 's3://social-system-test/instagram_graph_image_store/1/9991/'
images_df = ImageSchema.readImages(IMAGES_PATH)

# Show DataFrame
images_df.show()


try Eleventy
sudo python3 -m pip install pandas
sudo yum install git-all -y
sudo python3 -m pip install contextlib2 pillow
cd models/research/slim/
python download_and_convert_data.py --dataset_name=flowers --dataset_dir=


pyspark --packages org.apache.hadoop:hadoop-aws:2.7.3