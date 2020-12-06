import requests
import os
import json
import pyspark
from pyspark.sql import SparkSession

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'


def create_url():
    url = "https://data.smcgov.org/resource/mb6a-xn89.json"
    return url



def connect_to_endpoint(url):
    response = requests.request("GET", url)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def main():
    url = create_url()
    json_response = connect_to_endpoint(url)
    print(json.dumps(json_response, indent=4))
    with open("sample.json", "w+") as outfile: 
        #outfile.write((json_response))
        json.dump(json_response, outfile)


if __name__ == "__main__":
    main()
    spark = SparkSession.builder.appName("Molekule").config("spark.some.config.option", "some-value").getOrCreate()

#Reading data from the local and saving to the df
    df = spark.read.load("sample.json",format="json", inferSchema="true", header="true")
    df1 = df.withColumnRenamed(':@computed_region_i2t2_cryp','compregion_i2t2_cryp').withColumnRenamed(':@computed_region_uph5_8hpn','compregionuph5_8hpn')
    import pyspark.sql.functions as F
    from pyspark.sql.functions import col
    df2 = df1.select(F.col("location_1.coordinates").alias("coordinates"), F.col("location_1.type").alias("type"),F.col("location_1"))

    length = len(df2.select('coordinates').take(1)[0][0])
    df3 = df2.select([df2.coordinates] + [df2.coordinates[i] for i in range(length)])
    df2 = df2.join(df3, on = ['coordinates'], how = 'inner').drop('coordinates')
    df1 = df1.join(df2, on = ['location_1'], how = 'inner').drop('location_1')
    df4 = df1.withColumnRenamed('coordinates[0]','longitude').withColumnRenamed('coordinates[1]','latitude')
    df4.write.format('csv').option("header","true").mode('overwrite').save('output/output.csv')


