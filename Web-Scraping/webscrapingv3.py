import time
from datetime import datetime
import random
import pandas as pd
from newspaper import Article, build

# CORREÇÃO DO NOME: Usar hífens em vez de barras na data
hoje = datetime.now()
data_formatada = hoje.strftime('%d-%m-%y') 

# 1. Lista de portais - DICA: Use URLs de categorias para "forçar" o link a aparecer
portais = [
    {"nome": "revistaoeste", "url": "https://revistaoeste.com/politica/"},
]

def extrair_dados_portal(nome_portal, url_base):
    print(f"\n--- Iniciando coleta no portal: {nome_portal.upper()} ---")
    
    # Memoize=False limpa o cache. 
    # Adicionamos um User-Agent para parecer um navegador real e evitar os "0 links"
    jornal = build(url_base, language='pt', memoize_articles=False, browser_user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    urls_unicas = list(set([art.url for art in jornal.articles]))
    print(f"Foram encontrados {len(urls_unicas)} links únicos.")

    if len(urls_unicas) == 0:
        print(f"⚠ Aviso: Nenhum link encontrado para {nome_portal}. Pulando...")
        return

    dados = []
    
    for i, url in enumerate(urls_unicas):
        try:
            artigo = Article(url, language='pt', request_timeout=15)
            artigo.download()
            artigo.parse()
            
            texto_curto = artigo.text[:2000] if artigo.text else ""

            data = artigo.publish_date
            if data is None:
                data = artigo.meta_data.get('article:published_time', "Data Indisponível")

            dados.append({
                'titulo': artigo.title,
                'subtitulo': artigo.meta_description,
                'texto': texto_curto,
                'data': data,
                'url': url,
                'portal': nome_portal
            })
            
            print(f"[{nome_portal}] {i+1}/{len(urls_unicas)}: {artigo.title[:40]}...")

            time.sleep(random.uniform(1.0, 2.5))

            if (i + 1) % 15 == 0:
                pd.DataFrame(dados).to_csv(f'backup_{nome_portal}_{data_formatada}.csv', index=False)

        except Exception as e:
            continue

    df_final = pd.DataFrame(dados)
    # NOME DO ARQUIVO SEM BARRAS
    nome_arquivo = f'{nome_portal}_{data_formatada}.csv'
    df_final.to_csv(nome_arquivo, index=False)
    print(f"✔ Concluído! Arquivo '{nome_arquivo}' gerado com {len(df_final)} matérias.")

for portal in portais:
    extrair_dados_portal(portal['nome'], portal['url'])