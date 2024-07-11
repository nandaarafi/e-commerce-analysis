from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *



RAW_RESOURCES_PATH = f'raw_data/'
TRANSFORMED_RESOURCES_PATH = f'transformed_data/'
GCP_BUCKET_NAME = 'data-23539'
GCP_PROJECT_ID = 'master-engine-428507-f5'

def init() -> SparkSession:
    spark_init = SparkSession.builder \
        .appName('FinalProject') \
        .getOrCreate()
    return spark_init

def schema() -> (StructType, StructType) :
    product_info_schema = StructType([
        StructField('product_id', StringType(), True),
        StructField('product_name', StringType(), True),
        StructField('brand_id', IntegerType(), True),
        StructField('brand_name', StringType(), True),
        StructField('loves_count', IntegerType(), True),
        StructField('rating', FloatType(), True),
        StructField('reviews', IntegerType(), True),
        StructField('size', StringType(), True),
        StructField('variation_type', StringType(), True),
        StructField('variation_value', StringType(), True),
        StructField('variation_desc', StringType(), True),
        StructField('ingredients', StringType(), True),
        StructField('price_usd', FloatType(), True),
        StructField('value_price_usd', FloatType(), True),
        StructField('sale_price_usd', FloatType(), True),
        StructField('is_limited_edition', IntegerType(), True),
        StructField('is_new', IntegerType(), True),
        StructField('is_out_of_stock', IntegerType(), True),
        StructField('is_online_only', IntegerType(), True),
        StructField('is_sephora_exclusive', IntegerType(), True),
        StructField('highlights', StringType(), True),
        StructField('primary_category', StringType(), True),
        StructField('secondary_category', StringType(), True),
        StructField('tertiary_category', StringType(), True),
        StructField('child_count', IntegerType(), True),
        StructField('child_max_price', DoubleType(), True),
        StructField('child_min_price', DoubleType(), True),
    ])

    reviews_schema = StructType([
        StructField('review_id', IntegerType(), True),
        StructField('author_id', LongType(), True),
        StructField('rating', IntegerType(), True),
        StructField('is_recommended', FloatType(), True),
        StructField('helpfulness', FloatType(), True),
        StructField('total_feedback_count', IntegerType(), True),
        StructField('total_neg_feedback_count', IntegerType(), True),
        StructField('total_pos_feedback_count', IntegerType(), True),
        StructField('submission_time', StringType(), True),
        StructField('review_text', StringType(), True),
        StructField('review_title', StringType(), True),
        StructField('skin_tone', StringType(), True),
        StructField('eye_color', StringType(), True),
        StructField('skin_type', StringType(), True),
        StructField('hair_color', StringType(), True),
        StructField('product_id', StringType(), True),
        StructField('product_name', StringType(), True),
        StructField('brand_name', StringType(), True),
        StructField('price_usd', FloatType(), True),
    ])

    return product_info_schema, reviews_schema


def merge_reviews_data(spark_session: SparkSession, reviews_schema: StructType) -> DataFrame:
    file_pattern = f"gs://{GCP_BUCKET_NAME}/{RAW_RESOURCES_PATH}/reviews*.csv"
    try:
        df = spark_session.read.csv(file_pattern, header=True, schema=reviews_schema)
        return df
    except Exception as e:
        print(f"Error reading files: {e}")
        return None


def clean_transform(product_info: DataFrame, reviews: DataFrame) -> (DataFrame, DataFrame):
    """
    Cleans and transforms the product_info and reviews
    :param reviews:
    :param product_info:
    :return: tuple DataFrame
    """
    # Clean and Transform product_info
    columns_to_drop_product = ['variation_desc', 'value_price_usd', 'sale_price_usd']
    columns_to_cast_product = ['is_limited_edition', 'is_new', 'is_out_of_stock', 'is_online_only', 'is_sephora_exclusive']

    # Clean Product Info df
        # - cast BooleanType
        # - drop null value column
    for column in columns_to_cast_product:
        product_info = product_info.withColumn(column, col(column).cast(BooleanType()))
    clean_product_info = product_info.drop(*columns_to_drop_product)

    # Clean Reviews df
    clean_reviews_df = reviews.withColumn('is_recommended', col('is_recommended').cast(BooleanType()))

    return clean_product_info, clean_reviews_df


if __name__ == "__main__":
    #Spark Init
    spark_session = init()
    product_info_schema, reviews_schema = schema()

    product_info_df = spark_session.read.csv(f'gs://{GCP_BUCKET_NAME}/{RAW_RESOURCES_PATH}/product_info.csv',
                                             header=True,
                                             schema=product_info_schema)

    #Merge all review data
    reviews_df = merge_reviews_data(spark_session, reviews_schema)

    clean_product_info, clean_reviews_df = clean_transform(product_info_df, reviews_df)

    clean_product_info.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f'gs://{GCP_BUCKET_NAME}/{TRANSFORMED_RESOURCES_PATH}/product_info')

    # Write the cleaned reviews DataFrame to GCS in overwrite mode
    clean_reviews_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f'gs://{GCP_BUCKET_NAME}/{TRANSFORMED_RESOURCES_PATH}/reviews')

    spark_session.stop()


