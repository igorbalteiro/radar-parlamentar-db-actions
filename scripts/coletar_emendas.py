import os
import math
import requests
from datetime import datetime
from supabase import create_client
import re
import unicodedata

SSUPABASE_URL = os.environ["SUPABASE_URL"]
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
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0",
}


def parse_valor(valor):
    if not valor:
        return 0.0
    try:
        return float(str(valor).replace(".", "").replace(",", "."))
    except (ValueError, AttributeError):
        return 0.0


def normalizar_nome(nome):
    """
    Remove prefixo numérico, acentos e normaliza para minúsculas
    sem espaços extras para comparação entre as duas APIs.
    """
    if not nome:
        return ""
    # Remove prefixo do tipo "2737 - "
    nome = re.sub(r"^\d+\s*-\s*", "", nome)
    # Remove acentos e caracteres especiais
    nome = unicodedata.normalize("NFD", nome)
    nome = "".join(c for c in nome if unicodedata.category(c) != "Mn")
    return " ".join(nome.strip().lower().split())


def carregar_deputados():
    """
    Retorna um dict { nome_normalizado: id } com todos os deputados
    carregados da tabela local do Supabase — evita chamar a API da Câmara
    a cada execução, já que a tabela é populada pelo coletar_dados.py.
    """
    res = supabase.table("deputados").select("id, nome").execute()
    return {
        normalizar_nome(d["nome"]): d["id"]
        for d in res.data
    }


def buscar_pagina(offset):
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


def mapear_emenda(item, deputados_index):
    nome_normalizado = normalizar_nome(item.get("autor"))
    deputado_id = deputados_index.get(nome_normalizado)  # None se não encontrar

    return {
        "codigo_emenda": item.get("codigoEmenda"),
        "link_detalhamento": item.get("linkDetalhamento"),
        "ano": item.get("ano"),
        "tipo_emenda": item.get("tipoEmenda"),
        "autor": item.get("autor"),
        "nome_autor": item.get("nomeAutor"),
        "numero_emenda": item.get("numeroEmenda"),
        "localidade_do_gasto": item.get("localidadeDoGasto"),
        "funcao": item.get("funcao"),
        "subfuncao": item.get("subfuncao"),
        "programa": item.get("programa"),
        "acao": item.get("acao"),
        "plano_orcamentario": item.get("planoOrcamentario"),
        "valor_empenhado": parse_valor(item.get("valorEmpenhado")),
        "valor_liquidado": parse_valor(item.get("valorLiquidado")),
        "valor_pago": parse_valor(item.get("valorPago")),
        "valor_resto_inscrito": parse_valor(item.get("valorRestoInscrito")),
        "valor_resto_cancelado": parse_valor(item.get("valorRestoCancelado")),
        "valor_resto_pago": parse_valor(item.get("valorRestoPago")),
        "deputado_id": deputado_id,
        "coletado_em": datetime.today().isoformat(),
    }


def main():
    print(f"Iniciando coleta de emendas parlamentares — {ANO_ATUAL}")

    print("Carregando índice de deputados...")
    deputados_index = carregar_deputados()
    print(f"{len(deputados_index)} deputados carregados.")

    dados, total = buscar_pagina(offset=0)

    if total == 0:
        print("Nenhum registro encontrado.")
        return

    total_paginas = math.ceil(total / TAMANHO_PAGINA)
    print(f"{total} registros encontrados — {total_paginas} página(s) de {TAMANHO_PAGINA}")

    emendas = [mapear_emenda(item, deputados_index) for item in dados]

    for pagina in range(1, total_paginas):
        offset = pagina * TAMANHO_PAGINA
        print(f"Buscando página {pagina + 1}/{total_paginas} (offset {offset})...")
        dados, _ = buscar_pagina(offset=offset)
        emendas += [mapear_emenda(item, deputados_index) for item in dados]

    # Log de emendas sem match para facilitar diagnóstico
    sem_match = [e["nome_autor"] for e in emendas if e["deputado_id"] is None]
    if sem_match:
        unicos = sorted(set(sem_match))
        print(f"\n⚠️  {len(unicos)} autor(es) sem match com deputado:")
        for nome in unicos:
            print(f"   - {nome}")

    LOTE = 100
    total_inseridos = 0
    for i in range(0, len(emendas), LOTE):
        lote = emendas[i:i + LOTE]
        supabase.table("emendas_parlamentares").upsert(
            lote,
            on_conflict="codigo_emenda,ano,tipo_emenda,valor_empenhado"
        ).execute()
        total_inseridos += len(lote)
        print(f"  {total_inseridos}/{len(emendas)} registros salvos...")

    print(f"\n✅ Coleta finalizada — {len(emendas)} emendas armazenadas.")


if __name__ == "__main__":
    main()