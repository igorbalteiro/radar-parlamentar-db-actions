"""Coleta presenças anuais por dia pelo webservice de sessões da Câmara."""

import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from threading import local
from xml.etree import ElementTree

import requests
from supabase import create_client

from http_client import get

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

URL_DEPUTADOS = "https://www.camara.leg.br/SitCamaraWS/deputados.asmx/ObterDeputados"
URL_PRESENCAS_DIA = (
    "https://www.camara.leg.br/SitCamaraWS/sessoesreunioes.asmx/"
    "ListarPresencasDia"
)
TAMANHO_LOTE = 100
MAX_WORKERS = 3
TIMEOUT_WEBSERVICE = (10, 30)
TENTATIVAS_WEBSERVICE = 2

HEADERS = {
    "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "User-Agent": "Mozilla/5.0 (compatible; RadarParlamentar/1.0)",
}

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
_thread_local = local()

hoje = datetime.today()
ANO_ATUAL = hoje.year


def get_session():
    """Retorna uma sessão HTTP exclusiva para a thread atual."""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
    return _thread_local.session


def get_deputados_do_banco():
    """Retorna deputados cadastrados usando o ideCadastro como identificador."""
    res = supabase.table("deputados").select("id, nome").execute()
    return res.data


def get_id_legislatura(data_referencia):
    """Calcula a legislatura correspondente à data da sessão."""
    ano_inicio = data_referencia.year
    if (data_referencia.month, data_referencia.day) < (2, 1):
        ano_inicio -= 1
    ano_inicio -= (ano_inicio - 2023) % 4
    return 57 + (ano_inicio - 2023) // 4


def get_ids_por_carteira():
    """Mapeia carteiraParlamentar (matrícula) para ideCadastro."""
    resposta = get(
        URL_DEPUTADOS,
        headers=HEADERS,
        timeout=TIMEOUT_WEBSERVICE,
        tentativas=TENTATIVAS_WEBSERVICE,
        session=get_session(),
    )
    raiz = ElementTree.fromstring(resposta.content)
    ids_por_carteira = {}

    for deputado in raiz.findall("deputado"):
        ide_cadastro = deputado.findtext("ideCadastro")
        matricula = deputado.findtext("matricula")
        if ide_cadastro and matricula:
            ids_por_carteira[int(matricula)] = int(ide_cadastro)

    if not ids_por_carteira:
        raise RuntimeError("O webservice da Câmara não retornou matrículas de deputados.")
    return ids_por_carteira


def get_presencas_do_dia(data_sessao, ids_por_carteira):
    """Retorna frequências diárias associadas ao ideCadastro do deputado."""
    resposta = get(
        URL_PRESENCAS_DIA,
        params={
            "data": data_sessao.strftime("%d/%m/%Y"),
            "numLegislatura": get_id_legislatura(data_sessao),
            "numMatriculaParlamentar": "",
            "siglaPartido": "",
            "siglaUF": "",
        },
        headers=HEADERS,
        timeout=TIMEOUT_WEBSERVICE,
        tentativas=TENTATIVAS_WEBSERVICE,
        session=get_session(),
    )
    raiz = ElementTree.fromstring(resposta.content)
    totais = defaultdict(lambda: [0, 0, 0])

    for parlamentar in raiz.findall("./parlamentares/parlamentar"):
        carteira = parlamentar.findtext("carteiraParlamentar")
        deputado_id = ids_por_carteira.get(int(carteira)) if carteira else None
        if deputado_id is None:
            continue

        frequencia = (parlamentar.findtext("descricaoFrequenciaDia") or "").strip().casefold()
        if frequencia == "presença":
            totais[deputado_id][0] += 1
        elif frequencia == "ausência justificada":
            totais[deputado_id][1] += 1
        elif frequencia == "ausência":
            totais[deputado_id][2] += 1

    return totais


def iterar_dias_do_ano():
    """Gera todas as datas do ano até hoje, inclusive."""
    data_sessao = date(ANO_ATUAL, 1, 1)
    data_final = hoje.date()
    while data_sessao <= data_final:
        yield data_sessao
        data_sessao += timedelta(days=1)


def somar_totais(destino, origem):
    """Soma os totais de presença de uma data ao acumulado anual."""
    for deputado_id, valores in origem.items():
        acumulado = destino[deputado_id]
        for indice, valor in enumerate(valores):
            acumulado[indice] += valor


def montar_registros(deputados, totais):
    """Monta registros anuais de presença e a métrica diária correspondente."""
    data_referencia = hoje.strftime("%Y-%m-%d")
    registros_presencas = []
    registros_metricas = []

    for deputado in deputados:
        deputado_id = deputado["id"]
        presencas, justificadas, nao_justificadas = totais[deputado_id]
        total_sessoes = presencas + justificadas + nao_justificadas
        registros_presencas.append({
            "id_deputado": deputado_id,
            "ano": ANO_ATUAL,
            "total_sessoes": total_sessoes,
            "sessoes_presentes": presencas,
            "faltas_justificadas": justificadas,
            "faltas_nao_justificadas": nao_justificadas,
        })
        registros_metricas.append({
            "deputado_id": deputado_id,
            "data_referencia": data_referencia,
            "total_sessoes": total_sessoes,
            "sessoes_presentes": presencas,
        })

    return registros_presencas, registros_metricas


def main():
    print(f"Iniciando coleta de presenças — {ANO_ATUAL}", flush=True)

    deputados = get_deputados_do_banco()
    ids_do_banco = {deputado["id"] for deputado in deputados}
    ids_por_carteira = {
        carteira: deputado_id
        for carteira, deputado_id in get_ids_por_carteira().items()
        if deputado_id in ids_do_banco
    }
    if not ids_por_carteira:
        raise RuntimeError("Nenhuma matrícula da Câmara corresponde aos deputados no Supabase.")

    dias = list(iterar_dias_do_ano())
    print(
        f"{len(deputados)} deputados e {len(dias)} dias encontrados. "
        "Iniciando coleta diária em paralelo...\n",
        flush=True,
    )

    totais = defaultdict(lambda: [0, 0, 0])
    erros = []
    concluidos = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(get_presencas_do_dia, dia, ids_por_carteira): dia
            for dia in dias
        }
        for future in as_completed(futures):
            dia = futures[future]
            try:
                somar_totais(totais, future.result())
                concluidos += 1
                print(f"[{concluidos}/{len(dias)}] {dia:%d/%m/%Y}", flush=True)
            except Exception as erro:
                erros.append(dia)
                print(f"[ERRO] {dia:%d/%m/%Y}: {erro}", flush=True)

    registros_presencas, registros_metricas = montar_registros(deputados, totais)
    for inicio in range(0, len(registros_presencas), TAMANHO_LOTE):
        fim = inicio + TAMANHO_LOTE
        supabase.table("presencas_deputados").upsert(
            registros_presencas[inicio:fim],
            on_conflict="id_deputado,ano",
        ).execute()
        supabase.table("metricas_deputados").upsert(
            registros_metricas[inicio:fim],
            on_conflict="deputado_id,data_referencia",
        ).execute()

    print(f"\n✅ Concluído: {concluidos} dias processados e salvos em lotes.", flush=True)
    if erros:
        datas_com_erro = ", ".join(dia.strftime("%d/%m/%Y") for dia in erros)
        print(
            f"⚠️  Falhas transitórias em {len(erros)} dia(s); "
            f"a coleta parcial foi salva: {datas_com_erro}",
            flush=True,
        )


if __name__ == "__main__":
    main()
