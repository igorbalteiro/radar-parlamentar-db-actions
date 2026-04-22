import os
import requests
from datetime import datetime, timedelta
from supabase import create_client
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── Configuração ───────────────────────────────────────────────────────────

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
MAX_WORKERS = 5  # paralelas simultâneas — respeita o rate limit da API

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

hoje = datetime.today()
DATA_FIM = hoje.strftime("%Y-%m-%d")
DATA_INICIO = (hoje - timedelta(days=30)).strftime("%Y-%m-%d")


# ─── Funções de coleta ──────────────────────────────────────────────────────

def get_deputados():
    """Retorna lista com todos os 513 deputados ativos."""
    r = requests.get(f"{BASE_URL}/deputados", params={"itens": 513})
    r.raise_for_status()
    return r.json()["dados"]


def get_gastos(deputado_id):
    """
    Soma todos os gastos do deputado nos últimos 30 dias.
    A API de despesas filtra por ano e mês, não por data exata.
    """
    total = 0.0

    # Gera os meses que cobrem a janela de 30 dias
    meses = set()
    for i in range(31):
        dia = hoje - timedelta(days=i)
        meses.add((dia.year, dia.month))

    for ano, mes in meses:
        pagina = 1
        while True:
            r = requests.get(
                f"{BASE_URL}/deputados/{deputado_id}/despesas",
                params={
                    "ano": ano,
                    "mes": mes,
                    "itens": 100,
                    "pagina": pagina,
                },
            )
            r.raise_for_status()
            dados = r.json().get("dados", [])
            if not dados:
                break
            total += sum(d.get("valorLiquido", 0) for d in dados)
            pagina += 1

    return total


def get_discursos(deputado_id):
    """Conta quantos discursos o deputado fez nos últimos 30 dias, com paginação."""
    total = 0
    pagina = 1
    while True:
        r = requests.get(
            f"{BASE_URL}/deputados/{deputado_id}/discursos",
            params={
                "dataInicio": DATA_INICIO,
                "dataFim": DATA_FIM,
                "itens": 100,
                "pagina": pagina,
            },
        )
        r.raise_for_status()
        dados = r.json().get("dados", [])
        if not dados:
            break
        total += len(dados)
        pagina += 1
    return total


def get_proposicoes(deputado_id):
    """Conta proposições apresentadas pelo deputado nos últimos 30 dias, com paginação."""
    total = 0
    pagina = 1
    while True:
        r = requests.get(
            f"{BASE_URL}/proposicoes",
            params={
                "idDeputadoAutor": deputado_id,
                "dataApresentacaoInicio": DATA_INICIO,
                "dataApresentacaoFim": DATA_FIM,
                "itens": 100,
                "pagina": pagina,
            },
        )
        r.raise_for_status()
        dados = r.json().get("dados", [])
        if not dados:
            break
        total += len(dados)
        pagina += 1
    return total


# ─── Tarefa por deputado ────────────────────────────────────────────────────

def processar_deputado(dep):
    """
    Executada em paralelo para cada deputado.
    Coleta gastos, discursos e proposições, depois persiste no Supabase.
    Retorna o nome do deputado para log ou lança exceção em caso de falha.
    """
    dep_id = dep["id"]
    nome = dep["nome"]

    # Upsert do cadastro básico do deputado
    supabase.table("deputados").upsert({
        "id": dep_id,
        "nome": nome,
        "partido": dep.get("siglaPartido"),
        "uf": dep.get("siglaUf"),
        "atualizado_em": hoje.isoformat(),
    }).execute()

    # Coleta das métricas (cada chamada é independente)
    gastos = get_gastos(dep_id)
    discursos = get_discursos(dep_id)
    proposicoes = get_proposicoes(dep_id)

    # Upsert das métricas do dia — ignora duplicatas via constraint única
    supabase.table("metricas_deputados").upsert(
        {
            "deputado_id": dep_id,
            "data_referencia": DATA_FIM,
            "total_gastos": gastos,
            "qtd_discursos": discursos,
            "qtd_proposicoes": proposicoes,
        },
        on_conflict="deputado_id,data_referencia",
    ).execute()

    return nome


# ─── Execução principal ─────────────────────────────────────────────────────

def main():
    print(f"Iniciando coleta: {DATA_INICIO} → {DATA_FIM}")

    deputados = get_deputados()
    print(f"{len(deputados)} deputados encontrados. Iniciando coleta paralela...\n")

    concluidos = 0
    erros = []

    # ThreadPoolExecutor: mantém até MAX_WORKERS threads rodando ao mesmo tempo.
    # submit() agenda cada deputado como uma tarefa independente.
    # as_completed() itera conforme cada tarefa termina (não necessariamente em ordem).
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(processar_deputado, dep): dep["nome"]
            for dep in deputados
        }

        for future in as_completed(futures):
            nome = futures[future]
            try:
                future.result()  # relança exceção se a tarefa falhou
                concluidos += 1
                print(f"[{concluidos}/{len(deputados)}] {nome}")
            except Exception as e:
                erros.append(nome)
                print(f"[ERRO] {nome}: {e}")

    # Limpeza de registros com mais de 30 dias (janela rolante)
    supabase.table("metricas_deputados") \
        .delete() \
        .lt("data_referencia", DATA_INICIO) \
        .execute()

    print(f"\n✅ Concluído: {concluidos} deputados processados.")
    if erros:
        print(f"⚠️  Falhas ({len(erros)}): {', '.join(erros)}")


if __name__ == "__main__":
    main()