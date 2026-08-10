import os
from datetime import datetime
from supabase import create_client
from concurrent.futures import ThreadPoolExecutor, as_completed
from http_client import get_json

# ─── Configuração ───────────────────────────────────────────────────────────

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
# Consultas simultâneas demais tornam a API de dados abertos indisponível para
# o runner do GitHub Actions.
MAX_WORKERS = 2
TAMANHO_LOTE = 100
TENTATIVAS_LISTAGEM = 3

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

hoje = datetime.today()
DATA_HOJE = hoje.strftime("%Y-%m-%d")


# ─── Funções de coleta ──────────────────────────────────────────────────────

def get_deputados():
    """Retorna lista com todos os deputados da legislatura atual."""
    deputados = []
    pagina = 1
    while True:
        dados = get_json(
            f"{BASE_URL}/deputados",
            params={"idLegislatura": 57, "itens": 100, "pagina": pagina},
            timeout=(20, 60),
            tentativas=TENTATIVAS_LISTAGEM,
            atraso_maximo=8,
        )["dados"]
        deputados.extend(dados)
        if len(dados) < 100:
            return deputados
        pagina += 1

def get_status_deputado(deputado_id):
    """
    Busca o detalhe individual do deputado e retorna a situação atual do mandato.
    Ex: 'Exercício', 'Licença', 'Vacância', etc.
    """
    dados = get_json(f"{BASE_URL}/deputados/{deputado_id}").get("dados", {})
    return dados.get("ultimoStatus", {}).get("situacao")


def carregar_status_anteriores():
    """Permite manter o último status válido se a API falhar pontualmente."""
    res = supabase.table("deputados").select("id, status").execute()
    return {deputado["id"]: deputado.get("status") for deputado in res.data}


def carregar_deputados_do_banco():
    """Usa o último cadastro salvo se a API estiver indisponível por completo."""
    res = supabase.table("deputados").select(
        "id, nome, partido, uf, url_foto"
    ).execute()
    return [
        {
            "id": deputado["id"],
            "nome": deputado["nome"],
            "siglaPartido": deputado.get("partido"),
            "siglaUf": deputado.get("uf"),
            "urlFoto": deputado.get("url_foto"),
        }
        for deputado in res.data
    ]


# ─── Tarefa por deputado ────────────────────────────────────────────────────

def processar_deputado(
    dep,
    status_anteriores,
    atualizar_timestamp=True,
    consultar_status=True,
):
    """
    Executada em paralelo para cada deputado.
    Coleta o status e monta o cadastro para persistência em lote.
    """
    dep_id = dep["id"]
    nome = dep["nome"]

    if not consultar_status:
        status = status_anteriores.get(dep_id)
        aviso = None
    else:
        try:
            status = get_status_deputado(dep_id)
            aviso = None
        except Exception as erro:
            status = status_anteriores.get(dep_id)
            aviso = f"status não atualizado: {erro}"

    registro = {
        "id": dep_id,
        "nome": nome,
        "partido": dep.get("siglaPartido"),
        "uf": dep.get("siglaUf"),
        "url_foto": dep.get("urlFoto"),
        "status": status,
    }
    if atualizar_timestamp:
        registro["atualizado_em"] = hoje.isoformat()
    return registro, aviso


# ─── Execução principal ─────────────────────────────────────────────────────

def main():
    print(f"Iniciando coleta: {DATA_HOJE}")

    try:
        deputados_brutos = get_deputados()
        usando_cadastro_anterior = False
    except Exception as erro:
        print(f"⚠️  API da Câmara indisponível; usando cadastro anterior: {erro}")
        deputados_brutos = carregar_deputados_do_banco()
        usando_cadastro_anterior = True
        if not deputados_brutos:
            raise RuntimeError("A API da Câmara falhou e não há deputados salvos no banco.") from erro

    # A API pode repetir um deputado em páginas diferentes. O Postgres não
    # aceita duas linhas da mesma requisição de upsert com a mesma chave.
    deputados_por_id = {deputado["id"]: deputado for deputado in deputados_brutos}
    deputados = list(deputados_por_id.values())
    repetidos = len(deputados_brutos) - len(deputados)
    if repetidos:
        print(f"⚠️  {repetidos} registro(s) duplicado(s) da API foram ignorados.")

    status_anteriores = carregar_status_anteriores()
    if usando_cadastro_anterior:
        print(
            f"{len(deputados)} deputados encontrados no cadastro anterior. "
            "Nenhuma consulta individual de status será feita.\n"
        )
    else:
        print(f"{len(deputados)} deputados encontrados. Iniciando coleta controlada...\n")

    concluidos = 0
    avisos = []
    registros = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                processar_deputado,
                dep,
                status_anteriores,
                atualizar_timestamp=not usando_cadastro_anterior,
                consultar_status=not usando_cadastro_anterior,
            ): dep["nome"]
            for dep in deputados
        }

        for future in as_completed(futures):
            nome = futures[future]
            try:
                registro, aviso = future.result()
                registros.append(registro)
                if aviso:
                    avisos.append(f"{nome}: {aviso}")
                concluidos += 1
                print(f"[{concluidos}/{len(deputados)}] {nome}")
            except Exception as e:
                print(f"[ERRO] {nome}: {e}")
    for inicio in range(0, len(registros), TAMANHO_LOTE):
        supabase.table("deputados").upsert(registros[inicio:inicio + TAMANHO_LOTE]).execute()

    print(f"\n✅ Concluído: {concluidos} deputados processados e salvos em lotes.")
    if avisos:
        print(f"⚠️  Status mantido para {len(avisos)} deputado(s): {'; '.join(avisos)}")


if __name__ == "__main__":
    main()
