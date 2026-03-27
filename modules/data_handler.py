import pandas as pd
import glob
import os

def consolidar_e_limpar(nome_portal, pasta_origem="data/raw"):
    padrao = os.path.join(pasta_origem, f"{nome_portal}_*.csv")
    arquivos = glob.glob(padrao)
    
    if not arquivos:
        return None
    
    print(f"📂 Consolidando {len(arquivos)} arquivos de {nome_portal}...")
    dfs = [pd.read_csv(f) for f in arquivos]
    df_total = pd.concat(dfs, ignore_index=True)
    
    # Limpeza de duplicatas por URL (mantendo a primeira aparição)
    total_antes = len(df_total)
    df_limpo = df_total.drop_duplicates(subset=['url'], keep='first')
    
    print(f"♻️ Duplicatas removidas: {total_antes - len(df_limpo)}")
    return df_limpo