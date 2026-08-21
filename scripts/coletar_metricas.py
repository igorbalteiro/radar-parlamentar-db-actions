import os
from collections import defaultdict
from datetime import datetime, timedelta
from supabase import create_client
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import local
import requests
from http_client import get_json

# ─── Configuração ───────────────────────────────────────────────────────────

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
MAX_WORKERS = 4
TAMANHO_LOTE = 100
ITENS_POR_PAGINA = 100
TAMANHO_PAGINA_SUPABASE = 1000
TIMEOUT_API = (10, 20)
TENTATIVAS_API = 2

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

hoje = datetime.today()
DATA_FIM = hoje.strftime("%Y-%m-%d")
DATA_INICIO = (hoje - timedelta(days=30)).strftime("%Y-%m-%d")
_thread_local = local()


# ─── Funções de coleta ──────────────────────────────────────────────────────

def get_deputados_do_banco():
    """Retorna lista de deputados já cadastrados na tabela deputados do Supabase."""
    res = supabase.table("deputados").select("id, nome").execute()
    return res.data


def get_session():
    """Retorna uma sessão HTTP exclusiva para a thread atual."""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
    return _thread_local.session


def get_gastos_por_deputado():
    """
    Agrega gastos dos últimos 30 dias a partir da tabela já sincronizada.

    O job de despesas é executado antes deste. Consultar essa tabela evita uma
    requisição paginada à API da Câmara para cada deputado e para cada mês.
    """
    totais = defaultdict(float)
    inicio = 0

    while True:
        resposta = (
            supabase.table("despesas_deputados")
            .select("deputado_id, valor_documento")
            .gte("data_documento", DATA_INICIO)
            .lte("data_documento", DATA_FIM)
            .range(inicio, inicio + TAMANHO_PAGINA_SUPABASE - 1)
            .execute()
        )
        despesas = resposta.data
        for despesa in despesas:
            totais[despesa["deputado_id"]] += float(
                despesa.get("valor_documento") or 0
            )

        if len(despesas) < TAMANHO_PAGINA_SUPABASE:
            return totais
        inicio += TAMANHO_PAGINA_SUPABASE


def get_discursos(deputado_id):
    """Conta quantos discursos o deputado fez nos últimos 30 dias, com paginação."""
    total = 0
    pagina = 1
    while True:
        payload = get_json(
            f"{BASE_URL}/deputados/{deputado_id}/discursos",
            params={
                "dataInicio": DATA_INICIO,
                "dataFim": DATA_FIM,
                "itens": ITENS_POR_PAGINA,
                "pagina": pagina,
            },
            session=get_session(),
            timeout=TIMEOUT_API,
            tentativas=TENTATIVAS_API,
            atraso_maximo=2,
        )
        dados = payload.get("dados", [])
        total += len(dados)
        if len(dados) < ITENS_POR_PAGINA:
            break
        pagina += 1
    return total


def get_proposicoes(deputado_id):
    """Conta proposições apresentadas pelo deputado nos últimos 30 dias, com paginação."""
    total = 0
    pagina = 1
    while True:
        payload = get_json(
            f"{BASE_URL}/proposicoes",
            params={
                "idDeputadoAutor": deputado_id,
                "dataApresentacaoInicio": DATA_INICIO,
                "dataApresentacaoFim": DATA_FIM,
                "itens": ITENS_POR_PAGINA,
                "pagina": pagina,
            },
            session=get_session(),
            timeout=TIMEOUT_API,
            tentativas=TENTATIVAS_API,
            atraso_maximo=2,
        )
        dados = payload.get("dados", [])
        total += len(dados)
        if len(dados) < ITENS_POR_PAGINA:
            break
        pagina += 1
    return total


# ─── Tarefa por deputado ────────────────────────────────────────────────────

def processar_deputado(dep, gastos_por_deputado):
    """
    Executada em paralelo para cada deputado.
    Coleta métricas e persiste na tabela metricas_deputados.
    Retorna o nome do deputado para log ou lança exceção em caso de falha.
    """
    dep_id = dep["id"]
    nome = dep["nome"]

    gastos = gastos_por_deputado.get(dep_id, 0.0)
    discursos = get_discursos(dep_id)
    proposicoes = get_proposicoes(dep_id)

    return {
        "deputado_id": dep_id,
        "data_referencia": DATA_FIM,
        "total_gastos": gastos,
        "qtd_discursos": discursos,
        "qtd_proposicoes": proposicoes,
    }


# ─── Execução principal ─────────────────────────────────────────────────────

def main():
    print(f"Iniciando coleta: {DATA_INICIO} → {DATA_FIM}")

    deputados = get_deputados_do_banco()
    gastos_por_deputado = get_gastos_por_deputado()
    print(f"{len(deputados)} deputados encontrados no banco. Iniciando coleta paralela...\n")

    concluidos = 0
    erros = []
    registros = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(processar_deputado, dep, gastos_por_deputado): dep["nome"]
            for dep in deputados
        }

        for future in as_completed(futures):
            nome = futures[future]
            try:
                registros.append(future.result())
                concluidos += 1
                print(f"[{concluidos}/{len(deputados)}] {nome}")
            except Exception as e:
                erros.append(nome)
                print(f"[ERRO] {nome}: {e}")

    for inicio in range(0, len(registros), TAMANHO_LOTE):
        supabase.table("metricas_deputados").upsert(
            registros[inicio:inicio + TAMANHO_LOTE],
            on_conflict="deputado_id,data_referencia",
        ).execute()

    supabase.table("metricas_deputados") \
        .delete() \
        .lt("data_referencia", DATA_INICIO) \
        .execute()

    print(f"\n✅ Concluído: {concluidos} deputados processados.")
    if erros:
        print(
            f"⚠️  Falhas transitórias em {len(erros)} deputado(s); "
            f"a coleta parcial foi salva: {', '.join(erros)}"
        )


if __name__ == "__main__":
    main()
