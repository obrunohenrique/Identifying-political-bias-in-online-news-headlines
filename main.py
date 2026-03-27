import os
import pandas as pd
from modules.data_handler import consolidar_e_limpar
from modules.noise_killer import remover_ruido
from modules.llm_judge import rotular_vies

def executar_pipeline(nome_portal):
    print(f"\n🚀 Iniciando Pipeline para: {nome_portal.upper()}")
    
    # --- PASSO 1: CONSOLIDAÇÃO E LIMPEZA DE DUPLICADAS ---
    df_bruto = consolidar_e_limpar(nome_portal, pasta_origem="data/raw")
    
    if df_bruto is None or df_bruto.empty:
        print(f"❌ Erro: Nenhum arquivo '{nome_portal}_*.csv' encontrado em data/raw.")
        return

    # --- PASSO 2: ELIMINAÇÃO DE RUÍDO (POLÍTICA VS OUTROS) ---
    print(f"🧹 Filtrando conteúdo não-político via Llama 3.1...")
    df_limpo = remover_ruido(df_bruto)
    
    # SALVAMENTO OBRIGATÓRIO NA PASTA PROCESSED
    # Este arquivo é o seu "Gold Standard" sem rótulos.
    caminho_processed = f"data/processed/{nome_portal}.csv"
    df_limpo.to_csv(caminho_processed, index=False)
    
    total_instancias = len(df_limpo)
    print(f"✅ Etapa concluída! Dataset limpo salvo em: {caminho_processed}")
    print(f"📊 Volume final de política: {total_instancias} instâncias.")

    # --- VERIFICAÇÃO DE RESTRIÇÃO (TRAVA DE SEGURANÇA) ---
    if total_instancias < 500:
        print(f"⚠️  RESTRIÇÃO: Dataset com menos de 500 linhas.")
        print(f"🛑 A rotulagem para '{nome_portal}' foi abortada para preservar recursos.")
        return

    # --- PASSO 3: ROTULAGEM (LLM-AS-A-JUDGE) ---
    print(f"⚖️ Iniciando rotulagem de viés (Esquerda/Direita/Neutro)...")
    df_final = rotular_vies(df_limpo)
    
    # SALVAMENTO NA PASTA LABELED
    caminho_labeled = f"data/labeled/{nome_portal}_labeled.csv"
    df_final.to_csv(caminho_labeled, index=False)
    
    print(f"✨ SUCESSO! Dataset rotulado disponível em: {caminho_labeled}")

if __name__ == "__main__":
    # Garante a estrutura de pastas necessária
    for pasta in ["data/raw", "data/processed", "data/labeled"]:
        os.makedirs(pasta, exist_ok=True)
        
    portal = input("Qual portal deseja processar agora? ").strip().lower()
    executar_pipeline(portal)