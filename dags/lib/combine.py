from pyspark.sql.functions import to_utc_timestamp


def combine_formatted_data(weather_path, pollution_path, electricity_path, spark):
    df_weather = spark.read.parquet(weather_path)
    df_pollution = spark.read.parquet(pollution_path)
    df_electricity = spark.read.parquet(electricity_path)

    df_weather = df_weather.withColumn("date", to_utc_timestamp(df_weather["date"], "UTC"))
    df_electricity = df_electricity.withColumn("date", to_utc_timestamp(df_electricity["date"], "UTC"))
    df_pollution = df_pollution.withColumn("dt", to_utc_timestamp(df_pollution["dt"], "UTC"))

    '''df_combined = df_weather \
        .join(df_electricity, (df_weather["code_insee_region"] == "11") & (df_electricity["code_officiel_region"] == "11") & (
                df_weather["date"] == df_electricity["date"])) \
        .join(df_pollution, df_weather["date"] == df_pollution["dt"])'''

    df_combined = df_weather.filter(df_weather["code_insee_region"] == "11") \
        .join(df_pollution, df_weather["date"] == df_pollution["dt"], "left_outer") \
        .join(df_electricity, [
        (df_weather["date"] == df_electricity["date"]) &
        (df_weather["code_insee_region"] == "11") &
        (df_electricity["code_officiel_region"] == "11")
    ], "left_outer") \
        .select(df_weather["*"], df_pollution["*"], df_electricity["*"])

    df_combined = df_combined.drop("code_officiel_region")
    df_combined = df_combined.drop("nom_officiel_region")
    df_combined = df_combined.drop(df_electricity["date"])
    df_combined = df_combined.drop(df_pollution["dt"])
    df_combined.show()

    return df_combined
