import polars as pl
import os

csv_dir = "../../data_source/landing_area/csv_files"
parquet_dir = "../../data_source/bronze_area"

for nome_csv in os.listdir(csv_dir):
    if nome_csv.endswith(".csv"):
        caminho_csv = os.path.join(csv_dir, nome_csv)
        df = pl.read_csv(caminho_csv)

        nome_parquet = nome_csv.replace(".csv", ".parquet")
        caminho_parquet = os.path.join(parquet_dir, nome_parquet)

        df.write_parquet(caminho_parquet)
        print(f"{nome_parquet} salvo com sucesso!")
