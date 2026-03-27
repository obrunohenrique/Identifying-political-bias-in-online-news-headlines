import ollama
import pandas as pd
from tqdm import tqdm

def remover_ruido(df):
    tqdm.pandas(desc="🧹 Filtrando Política e Geopolítica")
    
    def verificar_politica(row):
        titulo = row['titulo'] if pd.notna(row['titulo']) else ""
        texto = row['texto'][:700] if pd.notna(row['texto']) else ""
        
        prompt = f"""
        Aja como um Editor-Chefe de Política e Assuntos Internacionais. 
        Filtre notícias que tenham relevância para o debate político brasileiro.

        MANTENHA (Responda 'SIM'):
        - Política Nacional (Governo, STF, Congresso, Partidos).
        - Geopolítica e Conflitos Internacionais (Israel-Palestina, Rússia-Ucrânia, etc).
        - Diplomacia Brasileira e posicionamentos do Itamaraty.
        - Discursos de líderes brasileiros sobre crises globais.
        - Impactos econômicos de guerras no Brasil (combustíveis, fertilizantes).

        ELIMINE (Responda 'NAO'):
        - Cotidiano, Gastronomia, Estilo de Vida ou Feriados.
        - Entretenimento, Cinema, Fofocas ou Esportes.
        - Saúde, Bem-estar ou Ciência genérica (sem relação com políticas públicas).
        - Notícias puramente técnicas de tecnologia ou arquitetura.

        NOTÍCIA:
        Título: {titulo}
        Texto: {texto}

        Responda APENAS 'SIM' ou 'NAO'.
        """
        
        try:
            response = ollama.generate(model='llama3.1', prompt=prompt)
            decisao = response['response'].strip().upper().replace(".", "")
            return "SIM" in decisao
        except Exception:
            return False

    df['is_politics'] = df.progress_apply(verificar_politica, axis=1)
    df_filtrado = df[df['is_politics'] == True].copy()
    
    return df_filtrado.drop(columns=['is_politics'])