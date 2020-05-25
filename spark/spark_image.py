from pyspark.sql import SparkSession
import numpy as np
import os
import pandas as pd
import cv2
from collections import Counter
from sklearn.cluster import KMeans
from pyspark.sql.functions import col, pandas_udf, udf, struct, PandasUDFType, split
from pyspark.sql.types import *

IMAGES_PATH = 's3a://social-system-test/instagram_graph_image_store/*/'


def color_many_classify(image, shape):
    """
    Returns most 6 most dominant colors in an image

    PARAMS: Image - nd Array of image pixel values of shape 1d
            shape - shape of pre transformed image.

    """
    # cluster and assign labels to the pixels
    clt = KMeans(n_clusters=6, n_init=20, precompute_distances=True).fit(image)

    # clt = KMeans(n_clusters = k).fit(image) <-- slower method with more steps in finding true cluster centers
    labels = clt.predict(image)

#     # count labels to find most popular
    label_counts = Counter(labels)

    # subset out most popular centroid
    dom_color_1 = clt.cluster_centers_[label_counts.most_common(1)[0][0]]
    dom_color_2 = clt.cluster_centers_[label_counts.most_common(2)[1][0]]
    dom_color_3 = clt.cluster_centers_[label_counts.most_common(3)[2][0]]
    dom_color_4 = clt.cluster_centers_[label_counts.most_common(4)[3][0]]
    dom_color_5 = clt.cluster_centers_[label_counts.most_common(5)[4][0]]
    dom_color_6 = clt.cluster_centers_[label_counts.most_common(6)[5][0]]

    # create RGB pixel values from cluster centers
    dom_color_1_hsv = np.full(shape, dom_color_1, dtype='uint8')
    dom_color_1_rgb = cv2.cvtColor(dom_color_1_hsv, cv2.COLOR_HSV2RGB)

    dom_color_2_hsv = np.full(shape, dom_color_2, dtype='uint8')
    dom_color_2_rgb = cv2.cvtColor(dom_color_2_hsv, cv2.COLOR_HSV2RGB)

    dom_color_3_hsv = np.full(shape, dom_color_3, dtype='uint8')
    dom_color_3_rgb = cv2.cvtColor(dom_color_3_hsv, cv2.COLOR_HSV2RGB)

    dom_color_4_hsv = np.full(shape, dom_color_4, dtype='uint8')
    dom_color_4_rgb = cv2.cvtColor(dom_color_4_hsv, cv2.COLOR_HSV2RGB)

    dom_color_5_hsv = np.full(shape, dom_color_5, dtype='uint8')
    dom_color_5_rgb = cv2.cvtColor(dom_color_5_hsv, cv2.COLOR_HSV2RGB)

    dom_color_6_hsv = np.full(shape, dom_color_6, dtype='uint8')
    dom_color_6_rgb = cv2.cvtColor(dom_color_6_hsv, cv2.COLOR_HSV2RGB)

    # concat input image and dom color square side by side for display
    # output_image = np.hstack((bgr_image[:,:,::-1], dom_color_1_rgb, dom_color_2_rgb, dom_color_3_rgb))

    hex1 = '#%02x%02x%02x' % (
        dom_color_1_rgb[0][0][0], dom_color_1_rgb[0][0][1], dom_color_1_rgb[0][0][2])
    hex2 = '#%02x%02x%02x' % (
        dom_color_2_rgb[0][0][0], dom_color_2_rgb[0][0][1], dom_color_2_rgb[0][0][2])
    hex3 = '#%02x%02x%02x' % (
        dom_color_3_rgb[0][0][0], dom_color_3_rgb[0][0][1], dom_color_3_rgb[0][0][2])
    hex4 = '#%02x%02x%02x' % (
        dom_color_4_rgb[0][0][0], dom_color_4_rgb[0][0][1], dom_color_4_rgb[0][0][2])
    hex5 = '#%02x%02x%02x' % (
        dom_color_5_rgb[0][0][0], dom_color_5_rgb[0][0][1], dom_color_5_rgb[0][0][2])
    hex6 = '#%02x%02x%02x' % (
        dom_color_6_rgb[0][0][0], dom_color_6_rgb[0][0][1], dom_color_6_rgb[0][0][2])

    # return list of dict
    color_list = [
        {'percentage': (label_counts.most_common(6)[0][1]/image.shape[0]),
            'red': int(dom_color_1_rgb[0][0][0]),
            'green': int(dom_color_1_rgb[0][0][1]),
            'blue': int(dom_color_1_rgb[0][0][2]),
            'hex': hex1
         },
        {
            'percentage': (label_counts.most_common(2)[1][1]/image.shape[0]),
            'red': int(dom_color_2_rgb[0][0][0]),
            'green': int(dom_color_2_rgb[0][0][1]),
            'blue': int(dom_color_2_rgb[0][0][2]),
            'hex': hex2
        },
        {
            'percentage': (label_counts.most_common(3)[2][1]/image.shape[0]),
            'red': int(dom_color_3_rgb[0][0][0]),
            'green': int(dom_color_3_rgb[0][0][1]),
            'blue': int(dom_color_3_rgb[0][0][2]),
            'hex': hex3
        },
        {
            'percentage': (label_counts.most_common(4)[3][1]/image.shape[0]),
            'red': int(dom_color_4_rgb[0][0][0]),
            'green': int(dom_color_4_rgb[0][0][1]),
            'blue': int(dom_color_4_rgb[0][0][2]),
            'hex': hex4
        },
        {
            'percentage': (label_counts.most_common(5)[4][1]/image.shape[0]),
            'red': int(dom_color_5_rgb[0][0][0]),
            'green': int(dom_color_5_rgb[0][0][1]),
            'blue': int(dom_color_5_rgb[0][0][2]),
            'hex': hex5
        },
        {
            'percentage': (label_counts.most_common(6)[5][1]/image.shape[0]),
            'red': int(dom_color_6_rgb[0][0][0]),
            'green': int(dom_color_6_rgb[0][0][1]),
            'blue': int(dom_color_6_rgb[0][0][2]),
            'hex': hex6
        }
    ]
    return color_list


attributes = [
    StructField('igId', StringType(), True),
    StructField('colors', StringType(), True)
]


@pandas_udf(StructType(attributes), PandasUDFType.GROUPED_MAP)
def convert_imageStruct(key, pdf):
    """
    Converts image Byte Array from Spark into a 3d vector, concats 
    all images of the pands dataframe in the group and classifies the 
    6 color centers of all images combined.

    PARAMS: 
    key : value that was used to groupby in Pands UDF Grouped_Map
    pdf : resulting pandas DF of grouping

    RETURNS: 
    Single row Pandas DF with key and string of dominant color values.
    """
    bgr_image = []
    for index, row in pdf.iterrows():
        if row['mode'] == 16:
            shape = (row['height'],
                     row['width'],
                     row['nChannels'])
            dtype = imageTypeByOrdinal(row['mode']).dtype
            image = np.ndarray(shape, dtype, row['data'])
            image = image[..., ::-1]
            image = cv2.resize(image, (75, 75), interpolation=cv2.INTER_AREA)
            if len(bgr_image) == 0:
                bgr_image = image
            else:
                bgr_image = np.concatenate((bgr_image, image), axis=0)
        else:
            pass

    try:
        shape = bgr_image.shape
        hsv_image = cv2.cvtColor(bgr_image, cv2.COLOR_BGR2HSV)
        image = hsv_image.reshape((hsv_image.shape[0] * hsv_image.shape[1], 3))
        colors = color_many_classify(image, shape)
    except:
        colors = ""
    return pd.DataFrame([[key[0], str(colors)]], columns=['igId', 'colors'])


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("Image Color Classification")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .getOrCreate()

    # Settings to allow pyarrow to work with Pandas UDF.
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    # Set a large batch size in practice.
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "6400")
    # Pass over corrupt images
    spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

    df = spark.read.format("image").option(
        "dropInvalid", True).load(IMAGES_PATH)

    # Flatten the Struct in the sprak df
    image_batch = df.select('image.origin',
                            'image.height',
                            'image.width',
                            'image.mode',
                            'image.nChannels',
                            'image.data')

    # Get The Instagram user ID from the folder in the image path
    image_batch = image_batch.withColumn('igId', split('origin', "/")[4])

    # First attempt to limit shuffling
    image_batch.repartition('igId')

    # Run the main conversion. Take Binary Array of image, convert it to nd.Array of pixels and KMeans on the colors.
    final = image_batch.groupby('igId').apply(convert_imageStruct)

    # TODO Using repartition again here or coalesce might limit shuffling but force the work to a single node.
    final.write.mode("overwrite").save(
        "s3a://social-system-test/spark/output/")

    # TODO
    # possibly figure out how to submit as an actual list or dict.

    spark.stop()
