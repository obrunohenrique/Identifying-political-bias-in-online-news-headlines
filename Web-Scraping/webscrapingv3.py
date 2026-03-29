import time
from datetime import datetime
import random
import pandas as pd
import os 
from newspaper import Article, build

# Garante que a pasta existe
PATH_RAW = "../data/raw"
os.makedirs(PATH_RAW, exist_ok=True)

hoje = datetime.now()
data_formatada = hoje.strftime('%d-%m-%y') 

portais = [
    {"nome": "gazetadopovo", "url": "https://www.gazetadopovo.com.br/ultimas-noticias/"},
    {"nome": "brasil247", "url": "https://www.brasil247.com/ultimas-noticias"},
    {"nome": "cnnbrasil", "url": "https://www.cnnbrasil.com.br/politica/"},
]

def extrair_dados_portal(nome_portal, url_base):
    print(f"\n--- Iniciando coleta no portal: {nome_portal.upper()} ---")
    
    jornal = build(url_base, language='pt', memoize_articles=False, 
                   browser_user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    urls_unicas = list(set([art.url for art in jornal.articles]))
    print(f"Foram encontrados {len(urls_unicas)} links únicos.")

    if len(urls_unicas) == 0:
        return

    dados = []
    
    for i, url in enumerate(urls_unicas):
        try:
            artigo = Article(url, language='pt', request_timeout=15)
            artigo.download()
            artigo.parse()
            
            texto_curto = artigo.text[:2000] if artigo.text else ""

            dados.append({
                'titulo': artigo.title,
                'subtitulo': artigo.meta_description,
                'texto': texto_curto,
                'data': artigo.publish_date if artigo.publish_date else "Data Indisponível",
                'url': url,
                'portal': nome_portal
            })
            
            print(f"[{nome_portal}] {i+1}/{len(urls_unicas)}: {artigo.title[:40]}...")
            time.sleep(random.uniform(1.0, 2.5))

            # Salva backup na pasta RAW
            if (i + 1) % 15 == 0:
                pd.DataFrame(dados).to_csv(os.path.join(PATH_RAW, f'backup_{nome_portal}_{data_formatada}.csv'), index=False)

        except Exception:
            continue

    if dados:
        df_final = pd.DataFrame(dados)
        # Salva o arquivo FINAL na pasta RAW
        nome_arquivo = os.path.join(PATH_RAW, f'{nome_portal}_{data_formatada}.csv')
        df_final.to_csv(nome_arquivo, index=False)
        print(f"✔ Concluído! Arquivo '{nome_arquivo}' gerado com {len(df_final)} matérias.")
        print(f"Se o script concluiu o Scraping, apague o Backup gerado, ele só contém duplicadas!")

for portal in portais:
    extrair_dados_portal(portal['nome'], portal['url'])