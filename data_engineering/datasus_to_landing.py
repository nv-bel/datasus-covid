import os
import pandas as pd
from dbfread import DBF

dbf_files = r"../../data_source/landing_area/dbf_files/"
csv_output = r"../../data_source/landing_area/csv_files/"
# os.makedirs(csv_output, exist_ok=True)

arquivos_dbf = [f for f in os.listdir(dbf_files) if f.endswith('.dbf')]

for arquivo_dbf in arquivos_dbf:
    caminho_dbf = os.path.join(dbf_files, arquivo_dbf)

    try:
        tabela = DBF(caminho_dbf, encoding='latin1')
        df = pd.DataFrame(iter(tabela))

        nome_csv = arquivo_dbf.replace('.dbf', '.csv')
        caminho_csv = os.path.join(csv_output, nome_csv)

        df.to_csv(caminho_csv, index=False, encoding='utf-8')
        print(f'{nome_csv} criado em {csv_output}')
    
    except Exception as e:
        print(f"Erro ao processar {arquivo_dbf}: {e}")

print('Conversão concluída!')