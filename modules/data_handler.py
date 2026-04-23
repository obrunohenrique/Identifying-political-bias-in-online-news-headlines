import pandas as pd
import glob
import os

def consolidar_novos_dados(nome_portal):
    # Pega tanto o arquivo final quanto os backups do portal no dia
    padrao_final = os.path.join("data/raw", f"{nome_portal}_*.csv")
    padrao_bkp = os.path.join("data/raw", f"backup_{nome_portal}_*.csv")
    
    arquivos = glob.glob(padrao_final) + glob.glob(padrao_bkp)
    
    if not arquivos:
        return None
    
    print(f"📂 Lendo {len(arquivos)} arquivos brutos novos...")
    dfs = [pd.read_csv(f) for f in arquivos]
    df_bruto = pd.concat(dfs, ignore_index=True)
    
    # Contagem de duplicadas baseada no TÍTULO
    total_antes = len(df_bruto)
    df_unico = df_bruto.drop_duplicates(subset=['titulo'], keep='last')
    duplicadas = total_antes - len(df_unico)
    
    print(f"♻️  Duplicatas removidas na união do RAW (por título): {duplicadas}")
    return df_unico