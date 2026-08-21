import os
from datetime import date, datetime, timedelta
from supabase import create_client
from concurrent.futures import ThreadPoolExecutor, as_completed
from http_client import get_json

# ─── Configuração ───────────────────────────────────────────────────────────

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
MAX_WORKERS = 8
TAMANHO_LOTE = 100

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

hoje = datetime.today()
DATA_FIM = hoje.strftime("%Y-%m-%d")
DATA_INICIO = (hoje - timedelta(days=30)).strftime("%Y-%m-%d")


# ─── Funções de coleta ──────────────────────────────────────────────────────

def get_deputados_do_banco():
    """Retorna lista de deputados já cadastrados na tabela deputados do Supabase."""
    res = supabase.table("deputados").select("id, nome").execute()
    return res.data


def get_id_legislatura(data_referencia):
    """Retorna a legislatura da Câmara correspondente à data informada."""
    if isinstance(data_referencia, datetime):
        data_referencia = data_referencia.date()

    ano_inicio = data_referencia.year
    if (data_referencia.month, data_referencia.day) < (2, 1):
        ano_inicio -= 1
    ano_inicio -= (ano_inicio - 2023) % 4

    return 57 + (ano_inicio - 2023) // 4


def get_gastos(deputado_id):
    """
    Soma todos os gastos do deputado nos últimos 30 dias.
    A API de despesas filtra por ano e mês, não por data exata.
    """
    total = 0.0

    meses = set()
    for i in range(31):
        dia = hoje - timedelta(days=i)
        meses.add((dia.year, dia.month))

    for ano, mes in meses:
        id_legislatura = get_id_legislatura(date(ano, mes, 1))
        pagina = 1
        while True:
            payload = get_json(
                f"{BASE_URL}/deputados/{deputado_id}/despesas",
                params={
                    "idLegislatura": id_legislatura,
                    "ano": ano,
                    "mes": mes,
                    "itens": 100,
                    "pagina": pagina,
                },
            )
            dados = payload.get("dados", [])
            if not dados:
                break
            total += sum(d.get("valorDocumento", 0) for d in dados)
            pagina += 1

    return total


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
                "itens": 100,
                "pagina": pagina,
            },
        )
        dados = payload.get("dados", [])
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
        payload = get_json(
            f"{BASE_URL}/proposicoes",
            params={
                "idDeputadoAutor": deputado_id,
                "dataApresentacaoInicio": DATA_INICIO,
                "dataApresentacaoFim": DATA_FIM,
                "itens": 100,
                "pagina": pagina,
            },
        )
        dados = payload.get("dados", [])
        if not dados:
            break
        total += len(dados)
        pagina += 1
    return total


# ─── Tarefa por deputado ────────────────────────────────────────────────────

def processar_deputado(dep):
    """
    Executada em paralelo para cada deputado.
    Coleta métricas e persiste na tabela metricas_deputados.
    Retorna o nome do deputado para log ou lança exceção em caso de falha.
    """
    dep_id = dep["id"]
    nome = dep["nome"]

    gastos = get_gastos(dep_id)
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
    print(f"{len(deputados)} deputados encontrados no banco. Iniciando coleta paralela...\n")

    concluidos = 0
    erros = []
    registros = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(processar_deputado, dep): dep["nome"]
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
