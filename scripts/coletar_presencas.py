import os
import random
import time
from datetime import datetime
from supabase import create_client
from bs4 import BeautifulSoup
import re
import requests
from http_client import get

# ─── Configuração ───────────────────────────────────────────────────────────

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

BASE_URL_CAMARA = "https://www.camara.leg.br/deputados"
# O Portal da Câmara passa a recusar conexões quando recebe muitas consultas
# seguidas do mesmo IP. A coleta é intencionalmente sequencial e espaçada.
TAMANHO_LOTE = 100
INTERVALO_MINIMO = 2.0
INTERVALO_MAXIMO = 3.0
PAUSA_APOS_TIMEOUT = 45
TENTATIVAS_POR_PAGINA = 3

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "User-Agent": "Mozilla/5.0 (compatible; RadarParlamentar/1.0)",
}

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
session = requests.Session()

hoje = datetime.today()
ANO_ATUAL = hoje.year


# ─── Funções de coleta ──────────────────────────────────────────────────────

def get_deputados_do_banco():
    """Retorna lista de deputados já cadastrados na tabela deputados do Supabase."""
    res = supabase.table("deputados").select("id, nome").execute()
    return res.data


def get_presencas_plenario(deputado_id):
    """
    Extrai o resumo anual do relatório oficial de Presença em Plenário.
    Retorna dict com presencas, ausencias_justificadas,
    ausencias_nao_justificadas e total_sessoes.
    """
    url = f"{BASE_URL_CAMARA}/{deputado_id}/presenca-plenario/{ANO_ATUAL}"
    try:
        r = get(
            url,
            headers=HEADERS,
            timeout=(10, 60),
            tentativas=TENTATIVAS_POR_PAGINA,
            session=session,
        )
    except requests.ConnectTimeout:
        # Evita que o próximo deputado recomece imediatamente uma sequência de
        # conexões recusadas/silenciosamente descartadas pela Câmara.
        print(f"  timeout de conexão; aguardando {PAUSA_APOS_TIMEOUT}s antes do próximo deputado")
        time.sleep(PAUSA_APOS_TIMEOUT)
        raise

    soup = BeautifulSoup(r.text, "html.parser")

    def extrair_valor(label):
        for linha in soup.find_all("tr"):
            colunas = [coluna.get_text(" ", strip=True) for coluna in linha.find_all(["th", "td"])]
            if colunas and label in colunas[0]:
                numero = re.search(r"\d+", " ".join(colunas[1:]))
                if numero:
                    return int(numero.group())
        return None

    presencas = extrair_valor("Total de dias com presença nas sessões deliberativas")
    ausencias_justificadas = extrair_valor(
        "Total de dias com ausências justificadas em sessões deliberativas"
    )
    ausencias_nao_justificadas = extrair_valor(
        "Total de dias com ausências não justificadas em sessões deliberativas"
    )
    if None in (presencas, ausencias_justificadas, ausencias_nao_justificadas):
        raise ValueError(f"Resumo de presença não encontrado em {url}")

    # Espaça acessos bem-sucedidos para não sobrecarregar o portal.
    time.sleep(random.uniform(INTERVALO_MINIMO, INTERVALO_MAXIMO))
    total_sessoes = presencas + ausencias_justificadas + ausencias_nao_justificadas

    return {
        "presencas": presencas,
        "ausencias_justificadas": ausencias_justificadas,
        "ausencias_nao_justificadas": ausencias_nao_justificadas,
        "total_sessoes": total_sessoes,
    }


# ─── Tarefa por deputado ────────────────────────────────────────────────────

def processar_deputado(dep):
    """
    Coleta os dados de um deputado.
    Coleta presenças e monta os registros para persistência em lote.
    """
    dep_id = dep["id"]
    presencas = get_presencas_plenario(dep_id)

    return {
        "presenca": {
            "id_deputado": dep_id,
            "ano": ANO_ATUAL,
            "total_sessoes": presencas["total_sessoes"],
            "sessoes_presentes": presencas["presencas"],
            "faltas_justificadas": presencas["ausencias_justificadas"],
            "faltas_nao_justificadas": presencas["ausencias_nao_justificadas"],
        },
        "metrica": {
            "deputado_id": dep_id,
            "data_referencia": hoje.strftime("%Y-%m-%d"),
            "total_sessoes": presencas["total_sessoes"],
            "sessoes_presentes": presencas["presencas"],
        },
    }


# ─── Execução principal ─────────────────────────────────────────────────────

def main():
    print(f"Iniciando coleta de presenças — {ANO_ATUAL}", flush=True)

    deputados = get_deputados_do_banco()
    print(f"{len(deputados)} deputados encontrados no banco. Iniciando coleta sequencial...\n", flush=True)

    concluidos = 0
    erros = []
    registros_presencas = []
    registros_metricas = []

    for indice, dep in enumerate(deputados, start=1):
        nome = dep["nome"]
        print(f"[{indice}/{len(deputados)}] Consultando {nome}...", flush=True)
        try:
            registros = processar_deputado(dep)
            registros_presencas.append(registros["presenca"])
            registros_metricas.append(registros["metrica"])
            concluidos += 1
            print(f"[{indice}/{len(deputados)}] ✓ {nome}", flush=True)
        except Exception as e:
            erros.append(nome)
            print(f"[ERRO] {nome}: {e}", flush=True)

    for inicio in range(0, len(registros_presencas), TAMANHO_LOTE):
        supabase.table("presencas_deputados").upsert(
            registros_presencas[inicio:inicio + TAMANHO_LOTE],
            on_conflict="id_deputado,ano",
        ).execute()
        supabase.table("metricas_deputados").upsert(
            registros_metricas[inicio:inicio + TAMANHO_LOTE],
            on_conflict="deputado_id,data_referencia",
        ).execute()

    print(f"\n✅ Concluído: {concluidos} deputados processados e salvos em lotes.", flush=True)
    if erros:
        print(
            f"⚠️  Falhas transitórias em {len(erros)} deputado(s); "
            f"a coleta parcial foi salva: {', '.join(erros)}",
            flush=True,
        )


if __name__ == "__main__":
    main()
