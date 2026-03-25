import pandas as pd
import os
import glob

# ==========================================
# CONFIGURAÇÃO
# ==========================================
# Nome base do portal que você quer consolidar
PORTAL_ALVO = "brasil247" 

# Pasta onde os arquivos estão ('.' significa a pasta atual do script)
PASTA_DADOS = "./" 

def consolidar_portal(nome_portal):
    # 1. Busca todos os arquivos que começam com o nome do portal e terminam em .csv
    # Ignora o arquivo final (ex: gazetadopovo.csv) para não entrar em loop
    padrao = os.path.join(PASTA_DADOS, f"{nome_portal}_*.csv")
    arquivos = glob.glob(padrao)
    
    if not arquivos:
        print(f"⚠ Nenhum arquivo encontrado para o padrão: {nome_portal}_*.csv")
        return

    print(f"📚 Encontrados {len(arquivos)} arquivos para o portal '{nome_portal.upper()}'.")
    
    lista_dfs = []
    
    for arquivo in arquivos:
        try:
            temp_df = pd.read_csv(arquivo)
            lista_dfs.append(temp_df)
            print(f"   -> Lendo: {arquivo} ({len(temp_df)} linhas)")
        except Exception as e:
            print(f"   ❌ Erro ao ler {arquivo}: {e}")

    # 2. Juntar todos os DataFrames
    df_consolidado = pd.concat(lista_dfs, ignore_index=True)
    total_antes = len(df_consolidado)

    # 3. Eliminar Duplicatas
    # Usamos a URL como chave única, pois o texto ou título podem ter variações mínimas, 
    # mas a URL de uma notícia é o seu "RG".
    df_consolidado = df_consolidado.drop_duplicates(subset=['url'], keep='first')
    total_depois = len(df_consolidado)
    duplicatas_removidas = total_antes - total_depois

    # 4. Salvar o arquivo final
    nome_saida = f"{nome_portal}.csv"
    df_consolidado.to_csv(nome_saida, index=False)

    print("\n--- RELATÓRIO DE CONSOLIDAÇÃO ---")
    print(f"✅ Arquivo gerado: {nome_saida}")
    print(f"📊 Total de instâncias brutas: {total_antes}")
    print(f"♻ Duplicatas removidas: {duplicatas_removidas}")
    print(f"🏆 Total de instâncias únicas (Dataset Final): {total_depois}")
    print("-" * 33)

# Execução
consolidar_portal(PORTAL_ALVO)