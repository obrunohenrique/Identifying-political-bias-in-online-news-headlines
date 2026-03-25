import pandas as pd
import ollama
import time

# ============================================
# Carrega o CSV que o seu time coletou(junção de todos os csv de um único portal)
# Lembrar de ajustar o nome do arquivo de saida para 'nome do portal_label_ia.csv'
# ============================================


df = pd.read_csv('gazetadopovo.csv')

def juiz_cego(row):
    # 1. Tratamento de campos vazios (NaN)
    titulo = row['titulo'] if pd.notna(row['titulo']) else "Sem título"
    subtitulo = row['subtitulo'] if pd.notna(row['subtitulo']) else ""
    # Pegamos apenas o começo do texto para evitar estourar o contexto
    texto = row['texto'][:1200] if pd.notna(row['texto']) else ""

    # 2. Montagem do prompt sem revelar a fonte
    prompt = f"""
    Analise o viés político do texto jornalístico brasileiro abaixo.
    Classifique apenas como: [ESQUERDA, DIREITA ou NEUTRO].
    
    CRITÉRIOS:
    - ESQUERDA: Foco em justiça social, críticas ao livre mercado, defesa de pautas progressistas.
    - DIREITA: Foco em valores conservadores, liberdade econômica, críticas ao progressismo.
    - NEUTRO: Relato puramente factual, sem adjetivos ideológicos ou escolha de palavras tendenciosas.

    CONTEÚDO:
    Título: {titulo}
    Subtítulo: {subtitulo}
    Início do Texto: {texto}

    Responda apenas com UMA PALAVRA (o rótulo).
    """

    try:
        response = ollama.generate(model='llama3.1', prompt=prompt)
        # Retorna apenas a palavra limpa em maiúsculo
        return response['response'].strip().upper().replace('.', '')
    except Exception as e:
        return "ERRO"

# Aplicando a função (isso vai demorar um pouco dependendo do tamanho do CSV)
print("🤖 O Llama 3.1 está vestindo a toga de juiz... Iniciando rotulagem.")
df['vies_ia'] = df.apply(juiz_cego, axis=1)

# Salva o resultado final
df.to_csv('gazetadopovo_label_ia.csv', index=False)
print("✅ Rotulagem concluída com sucesso!")