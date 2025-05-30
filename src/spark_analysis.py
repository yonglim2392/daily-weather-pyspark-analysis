from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, month, mean as _mean, stddev as _stddev, when, lit
import os
import glob
from statsmodels.tsa.seasonal import seasonal_decompose

# 1. Spark 세션 시작
spark = SparkSession.builder.appName("WeatherPreprocessing").getOrCreate()

# 2. 데이터 로드 및 병합
files = glob.glob("data/*.csv")
df_spark = spark.read.option("header", True).option("inferSchema", True).csv(files)
df_spark = df_spark.withColumn("DATE", to_date(col("DATE")))

# 3. 월 컬럼 추가
df_spark = df_spark.withColumn("month", month(col("DATE")))

# 4. 정합성 검사: Tmax < Tmin 제거
df_spark = df_spark.filter(col("MAX") >= col("MIN"))

# 5. 월별 평균 Tmax 계산
monthly_avg = df_spark.groupBy("STATION", "month").agg(_mean("MAX").alias("monthly_avg_tmax"))
monthly_avg_pd = monthly_avg.toPandas()

# 6. 이상치 탐지 및 처리 (Z-score)
# MAX 이상치
stats_max = df_spark.select(_mean("MAX").alias("mean_val_max"), _stddev("MAX").alias("std_val_max")).first()
mean_val_max, std_val_max = stats_max["mean_val_max"], stats_max["std_val_max"]

# MIN 이상치
stats_min = df_spark.select(_mean("MIN").alias("mean_val_min"), _stddev("MIN").alias("std_val_min")).first()
mean_val_min, std_val_min = stats_min["mean_val_min"], stats_min["std_val_min"]

# MAX Z-score 컬럼 추가
df_spark = df_spark.withColumn(
    "zscore_max",
    (col("MAX") - mean_val_max) / std_val_max
)

# MIN Z-score 컬럼 추가
df_spark = df_spark.withColumn(
    "zscore_min",
    (col("MIN") - mean_val_min) / std_val_min
)

# 이상치 저장용 (MAX 또는 MIN 이상치)
outliers = df_spark.filter(
    (col("zscore_max") > 3) | (col("zscore_max") < -3) |
    (col("zscore_min") > 3) | (col("zscore_min") < -3)
).toPandas()

# 이상치 NaN으로 대체 (평균값 대체 아님)
df_spark = df_spark.withColumn(
    "MAX",
    when((col("zscore_max") > 3) | (col("zscore_max") < -3), lit(None)).otherwise(col("MAX"))
)

df_spark = df_spark.withColumn(
    "MIN",
    when((col("zscore_min") > 3) | (col("zscore_min") < -3), lit(None)).otherwise(col("MIN"))
)

# 7. 시계열 분해 (한 지점 데이터 Pandas로 변환)
station_list = df_spark.select("STATION").distinct().limit(1).toPandas()["STATION"].tolist()
example_station = station_list[0]

station_df = df_spark.filter(col("STATION") == example_station).orderBy("DATE").toPandas()
station_df.set_index("DATE", inplace=True)

# 결측치 선형 보간 (시간 기반)
station_df[["MAX", "MIN"]] = station_df[["MAX", "MIN"]].interpolate(method='time')

# 보간 후 남은 결측값은 중간값(median)으로 대체
station_df["MAX"].fillna(station_df["MAX"].median(), inplace=True)
station_df["MIN"].fillna(station_df["MIN"].median(), inplace=True)

# 시계열 분해
decomposition = seasonal_decompose(station_df["MAX"], model='additive', period=30, extrapolate_trend='freq')

# 8. 저장 폴더 준비
os.makedirs("result", exist_ok=True)

# 9. CSV 저장
df_spark.toPandas().to_csv("result/cleaned_weather.csv", index=False)
monthly_avg_pd.to_csv("result/monthly_avg_tmax.csv", index=False)
outliers.to_csv("result/outliers.csv", index=False)

# 10. 시계열 분해 결과 저장
decomposition.trend.to_csv("result/ts_decompose_trend.csv")
decomposition.seasonal.to_csv("result/ts_decompose_seasonal.csv")
decomposition.resid.to_csv("result/ts_decompose_resid.csv")

# 11. 시계열 분해 시각화 저장
fig = decomposition.plot()
fig.set_size_inches(12, 8)
fig.savefig("result/ts_decompose_plot.png")

print("All results saved to 'result/' folder.")

# 종료
spark.stop()
