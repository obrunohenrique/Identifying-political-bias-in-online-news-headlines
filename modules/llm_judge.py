import ollama
import pandas as pd
from tqdm import tqdm

def rotular_vies(df):
    tqdm.pandas(desc="⚖️ Rotulando Viés Político")
    
    def classificar(row):
        titulo = row['titulo'] if pd.notna(row['titulo']) else ""
        subtitulo = row['subtitulo'] if pd.notna(row['subtitulo']) else ""
        texto = row['texto'][:1200] if pd.notna(row['texto']) else ""
        
        prompt = f"""
Analise o viés político do texto jornalístico brasileiro abaixo com base em marcadores ideológicos e geopolíticos.

CRITÉRIOS DE CLASSIFICAÇÃO:

1. ESQUERDA: 
- Geopolítica: Apoio à causa palestina, críticas a Israel, críticas ao "imperialismo americano" ou à OTAN. 
- Economia/Social: Foco em desigualdade social, defesa de serviços públicos, críticas ao mercado financeiro e termos como "justiça social", "soberania" e "direitos das minorias".

2. DIREITA: 
- Geopolítica: Alinhamento com Israel e Estados Unidos, críticas a regimes autoritários de esquerda (Venezuela, Cuba).
- Economia/Social: Foco em liberdade econômica, responsabilidade fiscal, valores conservadores, defesa da propriedade privada e críticas ao "estatismo" ou "pautas identitárias".

3. NEUTRO: 
- Linguagem puramente descritiva. Uso de verbos no indicativo (ex: "disse", "afirmou", "ocorreu") sem adjetivação ideológica. Apresenta os dois lados da mesma moeda com igual peso.

CONTEÚDO PARA ANÁLISE:
Título: {titulo}
Subtítulo: {subtitulo}
Texto: {texto[:1000]}

Responda estritamente com apenas uma palavra: [ESQUERDA, DIREITA ou NEUTRO].
"""
        
        try:
            response = ollama.generate(model='llama3.1', prompt=prompt)
            label = response['response'].strip().upper().replace(".", "")
            if label in ['ESQUERDA', 'DIREITA', 'NEUTRO']:
                return label
            return "INDEFINIDO"
        except:
            return "ERRO_CONEXAO"

    # Usamos o .progress_apply novamente
    df['vies_politico'] = df.progress_apply(classificar, axis=1)
    return df