import os
import requests
from datetime import datetime
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

ANO_ATUAL = datetime.today().year
TAMANHO_PAGINA = 30
BASE_URL = "https://portaldatransparencia.gov.br/emendas/consulta/resultado"
COLUNAS = ",".join([
    "linkDetalhamento", "ano", "tipoEmenda", "autor", "numeroEmenda",
    "possuiApoiadorSolicitante", "localidadeDoGasto", "funcao", "subfuncao",
    "programa", "acao", "planoOrcamentario", "codigoEmenda",
    "valorEmpenhado", "valorLiquidado", "valorPago",
    "valorRestoInscrito", "valorRestoCancelado", "valorRestoPago",
])

HEADERS = {
    # Necessário para o portal aceitar a requisição
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0",
}


def parse_valor(valor):
    """Converte string monetária brasileira para float. Ex: '1.234,56' → 1234.56"""
    if not valor:
        return 0.0
    try:
        return float(str(valor).replace(".", "").replace(",", "."))
    except (ValueError, AttributeError):
        return 0.0


def buscar_pagina(offset):
    """Busca uma página de emendas e retorna (dados, total_de_registros)."""
    params = {
        "paginacaoSimples": "false",
        "tamanhoPagina": TAMANHO_PAGINA,
        "offset": offset,
        "direcaoOrdenacao": "asc",
        "colunaOrdenacao": "autor",
        "de": ANO_ATUAL,
        "ate": ANO_ATUAL,
        "colunasSelecionadas": COLUNAS,
    }
    r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    payload = r.json()
    return payload.get("data", []), payload.get("recordsTotal", 0)


def mapear_emenda(item):
    """Transforma o dict da API no formato da tabela do Supabase."""
    return {
        "codigo_emenda": item.get("codigoEmenda"),
        "ano": item.get("ano"),
        "tipo_emenda": item.get("tipoEmenda"),
        "autor": item.get("autor"),
        "nome_autor": item.get("nomeAutor"),
        "numero_emenda": item.get("numeroEmenda"),
        "localidade_do_gasto": item.get("localidadeDoGasto"),
        "funcao": item.get("funcao"),
        "subfuncao": item.get("subfuncao"),
        "valor_empenhado": parse_valor(item.get("valorEmpenhado")),
        "valor_liquidado": parse_valor(item.get("valorLiquidado")),
        "valor_pago": parse_valor(item.get("valorPago")),
        "valor_resto_inscrito": parse_valor(item.get("valorRestoInscrito")),
        "valor_resto_cancelado": parse_valor(item.get("valorRestoCancelado")),
        "valor_resto_pago": parse_valor(item.get("valorRestoPago")),
        "coletado_em": datetime.today().isoformat(),
    }


def main():
    print(f"Iniciando coleta de emendas parlamentares — {ANO_ATUAL}")

    # Primeira requisição para descobrir o total de registros
    dados, total = buscar_pagina(offset=0)

    if total == 0:
        print("Nenhum registro encontrado.")
        return

    # Calcula quantas páginas existem
    import math
    total_paginas = math.ceil(total / TAMANHO_PAGINA)
    print(f"{total} registros encontrados — {total_paginas} página(s) de {TAMANHO_PAGINA}")

    # Processa a primeira página já carregada
    emendas = [mapear_emenda(item) for item in dados]

    # Itera pelas demais páginas incrementando o offset
    for pagina in range(1, total_paginas):
        offset = pagina * TAMANHO_PAGINA
        print(f"Buscando página {pagina + 1}/{total_paginas} (offset {offset})...")

        dados, _ = buscar_pagina(offset=offset)
        emendas += [mapear_emenda(item) for item in dados]

    # Persiste em lotes de 100 para não sobrecarregar o Supabase
    LOTE = 100
    total_inseridos = 0
    for i in range(0, len(emendas), LOTE):
        lote = emendas[i:i + LOTE]
        supabase.table("emendas_parlamentares").upsert(
            lote,
            on_conflict="codigo_emenda,ano"
        ).execute()
        total_inseridos += len(lote)
        print(f"  {total_inseridos}/{len(emendas)} registros salvos...")

    print(f"\n✅ Coleta finalizada — {len(emendas)} emendas armazenadas.")


if __name__ == "__main__":
    main()