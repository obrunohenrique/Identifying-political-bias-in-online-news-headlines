import ollama
import pandas as pd
from tqdm import tqdm
import re

def rotular_vies(df):
    """
    Script de rotulagem com foco em 'Framing' (Enquadramento).
    Extrai o rótulo político e a justificativa lógica da LLM.
    """
    tqdm.pandas(desc="⚖️ Analisando Enquadramento Político")
    
    def extrair_informacoes(texto_bruto):
        """Mina o rótulo e a justificativa dentro da resposta da LLM."""
        texto_limpo = texto_bruto.strip()
        
        # Inicializa valores padrão caso a extração falhe
        justificativa = "Justificativa não identificada."
        rotulo = "INDEFINIDO"
        
        # 1. Captura a Justificativa (busca após a tag JUSTIFICATIVA:)
        match_just = re.search(r"JUSTIFICATIVA:\s*(.*)", texto_limpo, re.IGNORECASE)
        if match_just:
            # Pega apenas a primeira linha da justificativa para evitar poluição
            justificativa = match_just.group(1).split('\n')[0].strip()
            
        # 2. Captura o Rótulo (busca o termo dentro de colchetes ou após a tag ROTULO:)
        match_rot = re.search(r"ROTULO:\s*\[?(ESQUERDA|DIREITA|NEUTRO)\]?", texto_limpo, re.IGNORECASE)
        
        if match_rot:
            rotulo = match_rot.group(1).upper()
        else:
            # Fallback: Busca as palavras-chave no texto caso a tag esteja mal formatada
            texto_upper = texto_limpo.upper()
            if "ESQUERDA" in texto_upper: rotulo = "ESQUERDA"
            elif "DIREITA" in texto_upper: rotulo = "DIREITA"
            elif "NEUTRO" in texto_upper: rotulo = "NEUTRO"
            
        return rotulo, justificativa

    def classificar(row):
        # Preparação dos dados da linha
        titulo = str(row['titulo']) if pd.notna(row['titulo']) else ""
        subtitulo = str(row['subtitulo']) if pd.notna(row['subtitulo']) else ""
        texto_limpo = str(row['texto']).replace('\n', ' ')[:1000] if pd.notna(row['texto']) else ""
        
        # O SEU NOVO PROMPT (Focado em Framing e Exemplos)
        prompt = f"""
Aja como um analista sênior de comunicação política especializado em enquadramento midiático (framing). Sua tarefa é identificar o viés ideológico de notícias brasileiras.

### INSTRUÇÕES DE PENSAMENTO:
Antes de rotular, pergunte-se: "A quem esta informação beneficia ou quem ela tenta desgastar?". 
- Noticiar fatos negativos de figuras de esquerda ou destacar que seus parentes são militantes é um enquadramento típico de desgaste usado pela DIREITA.
- Noticiar lucros recordes de bancos ou criticar a atuação de forças policiais é um enquadramento típico de desgaste usado pela ESQUERDA.

### CRITÉRIOS DE CLASSIFICAÇÃO:
1. ESQUERDA: Foco em justiça social, críticas ao mercado, defesa de pautas progressistas, críticas a Israel/EUA ou apoio a movimentos de base.
2. DIREITA: Foco em liberdade econômica, valores conservadores, defesa da propriedade, críticas a governos de esquerda (Venezuela/Cuba) ou questionamento de instituições/magistrados sob uma ótica conservadora.
3. NEUTRO: Apenas se o texto for puramente institucional e factual, sem adjetivação ou seleção de fatos que induzam a uma conclusão negativa sobre um espectro político.

### EXEMPLOS DE "PULO DO GATO":
- Texto: "Irmão de Ministro é militante de esquerda" -> Rótulo: DIREITA (Uso de fato pessoal para questionar a imparcialidade de uma autoridade).
- Texto: "Bancos lucram bilhões enquanto fome cresce" -> Rótulo: ESQUERDA (Associação de lucro privado à desigualdade social).

### CONTEÚDO PARA ANÁLISE:
Título: {titulo}
Subtítulo: {subtitulo}
Texto: {texto_limpo}

### FORMATO DE RESPOSTA:
JUSTIFICATIVA: (Explique o enquadramento em uma frase)
ROTULO: [ESQUERDA, DIREITA ou NEUTRO]
"""
        
        try:
            # Execução via Ollama
            response = ollama.generate(model='llama3.1', prompt=prompt)
            resposta_bruta = response['response']
            
            # Processamento da resposta estruturada
            label, justification = extrair_informacoes(resposta_bruta)
            
            return pd.Series([label, justification], index=['vies_politico', 'justificativa_llm'])
        
        except Exception as e:
            return pd.Series(["ERRO_CONEXAO", str(e)], index=['vies_politico', 'justificativa_llm'])

    # Aplica a função criando as duas novas colunas no DataFrame
    df[['vies_politico', 'justificativa_llm']] = df.progress_apply(classificar, axis=1)
    
    return df