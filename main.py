from pyspark.sql import SparkSession
from pathlib import Path

def main():
    data_path = Path.cwd() / 'data'
    spark = SparkSession.builder.master('local').getOrCreate()
    spark \
        .read.csv(str(data_path / 'MICRODADOS_ENEM_2019.csv')) \
        .write.mode('overwrite').parquet(str(data_path / 'microdados.parquet'))

if __name__ == "__main__":
    main()