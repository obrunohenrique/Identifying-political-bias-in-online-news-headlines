import os
import shutil
import pandas as pd
from modules.data_handler import consolidar_novos_dados
from modules.noise_killer import remover_ruido
from modules.llm_judge import rotular_vies

def executar_pipeline_incremental(nome_portal):
    print(f"\n--- PIPELINE INCREMENTAL: {nome_portal.upper()} ---")
    
    # 1. PEGAR NOVOS DADOS
    df_novos = consolidar_novos_dados(nome_portal)
    if df_novos is None or df_novos.empty:
        print("☕ Nada novo para processar.")
        return

    # 2. FILTRAR RUÍDO (Barra de progresso aparece aqui)
    print(f"🧠 Llama 3.1 analisando relevância dos novos dados...")
    df_novos_limpos = remover_ruido(df_novos)

    # 3. UNIR COM O QUE JÁ ESTAVA NA PASTA PROCESSED
    path_proc = f"data/processed/{nome_portal}.csv"
    if os.path.exists(path_proc):
        df_antigo = pd.read_csv(path_proc)
        df_consolidado = pd.concat([df_antigo, df_novos_limpos], ignore_index=True)
        # Limpeza final para garantir que nada escapou
        df_consolidado = df_consolidado.drop_duplicates(subset=['url'], keep='first')
    else:
        df_consolidado = df_novos_limpos

    # 4. SALVAR E ARQUIVAR
    df_consolidado.to_csv(path_proc, index=False)
    print(f"💾 Base de dados processada atualizada! Total: {len(df_consolidado)} linhas.")

    # Mover arquivos usados para Archive para não reprocessar na próxima vez
    arquivar_raw(nome_portal)

    # 5. VERIFICAR META DE 500 PARA ROTULAR
    total_total = len(df_consolidado)
    if total_total < 500:
        print(f"⚠️  Volume atual: {total_total}/500. Continue o scraping!")
        return

    # 6. ROTULAGEM (Barra de progresso aparece aqui)
    print(f"⚖️ Alvo atingido! Iniciando Juiz de Viés...")
    df_labeled = rotular_vies(df_consolidado)
    df_labeled.to_csv(f"data/labeled/{nome_portal}_labeled.csv", index=False)
    print("✨ SUCESSO! Dataset final pronto para o BERTimbau.")

def arquivar_raw(nome_portal):
    os.makedirs("data/archive", exist_ok=True)
    import glob
    itens = glob.glob(f"data/raw/{nome_portal}_*.csv") + glob.glob(f"data/raw/backup_{nome_portal}_*.csv")
    for f in itens:
        shutil.move(f, os.path.join("data/archive", os.path.basename(f)))

if __name__ == "__main__":
    portal = input("Qual portal processar? ").strip().lower()
    executar_pipeline_incremental(portal)