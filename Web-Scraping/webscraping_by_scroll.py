import asyncio
from playwright.async_api import async_playwright
from newspaper import Article
import pandas as pd
import time
import random
from datetime import datetime

# python -m playwright install

# =========================
# CONFIG DOS PORTAIS
# =========================

PORTAIS = [

    {
        "nome": "ValorEconomico",
        "url": "https://valor.globo.com/politica/",
        "botao": "Veja mais",
        "dominio": "valor.globo.com",
        "tipo": "scroll",
        "max_iter": 50,
        "seletor": "a[href]"
    },

    {
        "nome": "Poder360",
        "url": "https://www.poder360.com.br/poder-governo/",
        "botao": "Carregar Mais",
        "dominio": "poder360.com.br",
        "tipo": "scroll",
        "max_iter": 50,
        "seletor": "a[href]"
    }
]

# =========================
# FILTROS
# =========================

def is_artigo_valido(href, dominio):
    return (
        href
        and dominio in href
        and "page" not in href
        and "#" not in href
        and "wp-content" not in href
        and href.count("/") >= 4
    )

def is_artigo_antagonista(href):
    return (
        href
        and "oantagonista.com.br" in href
        and "/brasil/" in href
        and "page" not in href
        and "#" not in href
        and href.count("/") >= 5
    )

# =========================
# SCROLL
# =========================

async def coletar_scroll(config):
    links = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled"]
        )

        page = await browser.new_page(user_agent="Mozilla/5.0")

        await page.goto(config["url"], timeout=60000)
        await page.wait_for_timeout(5000)

        for i in range(config["max_iter"]):
            print(f"Iteração {i+1}")

            await page.mouse.wheel(0, 6000)
            await page.wait_for_timeout(random.randint(1500, 3000))

            if config.get("botao"):
                try:
                    botao = await page.query_selector(f"text={config['botao']}")
                    if botao:
                        await botao.click()
                        print(f"Cliquei em {config['botao']}")
                        await page.wait_for_timeout(3000)
                except:
                    pass

        hrefs = await page.eval_on_selector_all(
            config["seletor"],
            "els => els.map(e => e.getAttribute('href'))"
        )

        for href in hrefs:
            if not href:
                continue

            if href.startswith("/"):
                href = f"https://{config['dominio']}" + href

            if is_artigo_valido(href, config["dominio"]):
                links.add(href)

        await browser.close()

    return list(links)

# =========================
# PAGINAÇÃO COM CLIQUE REAL
# =========================

async def coletar_paginacao_click(config):
    print(f"\n--- Coletando {config['nome']} via paginação real ---")

    links = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled"]
        )

        page = await browser.new_page(user_agent="Mozilla/5.0")

        await page.goto(config["url"], timeout=60000)
        await page.wait_for_timeout(5000)

        pagina_atual = 1

        for _ in range(config["paginas"]):
            print(f"Página {pagina_atual}")

            hrefs = await page.eval_on_selector_all(
                config["seletor"],
                "els => els.map(e => e.getAttribute('href'))"
            )

            for href in hrefs:
                if not href:
                    continue

                if href.startswith("/"):
                    href = f"https://{config['dominio']}" + href

                # filtro específico
                if config["nome"] == "OAntagonista":
                    if is_artigo_antagonista(href):
                        links.add(href)
                else:
                    if is_artigo_valido(href, config["dominio"]):
                        links.add(href)

            print(f"Total acumulado: {len(links)}")

            # próxima página (clicando no número)
            proxima = str(pagina_atual + 1)

            try:
                botao = await page.query_selector(f"text={proxima}")

                if botao:
                    await botao.click()
                    pagina_atual += 1

                    await page.wait_for_load_state("networkidle")
                    await page.wait_for_timeout(random.randint(4000, 7000))
                else:
                    print("Fim da paginação")
                    break

            except:
                print("Erro ao clicar")
                break

        await browser.close()

    return list(links)

# =========================
# ROUTER
# =========================

async def coletar_links(config):
    print(f"\n--- Coletando {config['nome']} ---")

    if config["tipo"] == "scroll":
        links = await coletar_scroll(config)

    elif config["tipo"] == "paginacao_click":
        links = await coletar_paginacao_click(config)

    else:
        print("Tipo não suportado")
        return []

    print(f"{len(links)} links coletados")
    return links

# =========================
# EXTRAÇÃO
# =========================

def extrair_artigos(links, nome_portal):
    print(f"\n--- Extraindo {nome_portal} ---")

    dados = []
    hoje = datetime.now().strftime('%d-%m-%y')

    for i, url in enumerate(links):
        try:
            art = Article(url, language='pt')
            art.download()
            art.parse()

            dados.append({
                "titulo": art.title,
                "texto": art.text[:2000],
                "data": art.publish_date,
                "url": url,
                "portal": nome_portal
            })

            print(f"[{i+1}/{len(links)}] {art.title[:60]}")

            time.sleep(random.uniform(1, 3))

        except:
            print(f"Erro em: {url[:60]}")
            continue

    df = pd.DataFrame(dados)
    df.drop_duplicates(subset="url", inplace=True)

    nome_arquivo = f"{nome_portal}_{hoje}.csv"
    df.to_csv(nome_arquivo, index=False)

    print(f"\n{nome_portal}: {len(df)} matérias salvas")

# =========================
# PIPELINE
# =========================

async def executar_todos():
    for config in PORTAIS:
        links = await coletar_links(config)

        if len(links) == 0:
            print(f"Nenhum link encontrado para {config['nome']}")
            continue

        # extrair_artigos(links, config["nome"])

# =========================
# EXECUÇÃO
# =========================

if __name__ == "__main__":
    asyncio.run(executar_todos())