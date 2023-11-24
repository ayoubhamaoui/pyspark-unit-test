import pyspark.sql.functions as F
def transform_1(fact_df, lookup_df):

    return fact_df.join(lookup_df, 'pincode')

def transform_2(fact_df):
   return fact_df.groupBy("customer_id").count().where(F.col("count")>1)