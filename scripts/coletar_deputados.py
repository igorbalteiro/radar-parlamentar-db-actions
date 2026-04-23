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
DATA_HOJE = hoje.strftime("%Y-%m-%d")


# ─── Funções de coleta ──────────────────────────────────────────────────────

def get_deputados():
    """Retorna lista com todos os deputados da legislatura atual."""
    r = requests.get(f"{BASE_URL}/deputados?idLegislatura=57")
    r.raise_for_status()
    return r.json()["dados"]

def get_status_deputado(deputado_id):
    """
    Busca o detalhe individual do deputado e retorna a situação atual do mandato.
    Ex: 'Exercício', 'Licença', 'Vacância', etc.
    """
    r = requests.get(f"{BASE_URL}/deputados/{deputado_id}")
    r.raise_for_status()
    dados = r.json().get("dados", {})
    return dados.get("ultimoStatus", {}).get("situacao")


# ─── Tarefa por deputado ────────────────────────────────────────────────────

def processar_deputado(dep):
    """
    Executada em paralelo para cada deputado.
    Coleta gastos, discursos e proposições, depois persiste no Supabase.
    Retorna o nome do deputado para log ou lança exceção em caso de falha.
    """
    dep_id = dep["id"]
    nome = dep["nome"]

    status = get_status_deputado(dep_id)

    # Upsert do cadastro básico do deputado
    supabase.table("deputados").upsert({
        "id": dep_id,
        "nome": nome,
        "partido": dep.get("siglaPartido"),
        "uf": dep.get("siglaUf"),
        "url_foto": dep.get("urlFoto"),
        "status": status,
        "atualizado_em": hoje.isoformat(),
    }).execute()

    return nome


# ─── Execução principal ─────────────────────────────────────────────────────

def main():
    print(f"Iniciando coleta: {DATA_HOJE}")

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


    print(f"\n✅ Concluído: {concluidos} deputados processados.")
    if erros:
        print(f"⚠️  Falhas ({len(erros)}): {', '.join(erros)}")


if __name__ == "__main__":
    main()