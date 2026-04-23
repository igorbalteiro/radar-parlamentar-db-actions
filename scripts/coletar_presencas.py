import os
import requests
from datetime import datetime
from supabase import create_client
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

# ─── Configuração ───────────────────────────────────────────────────────────

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

BASE_URL_CAMARA = "https://www.camara.leg.br/deputados"
MAX_WORKERS = 5

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

hoje = datetime.today()
ANO_ATUAL = hoje.year


# ─── Funções de coleta ──────────────────────────────────────────────────────

def get_deputados_do_banco():
    """Retorna lista de deputados já cadastrados na tabela deputados do Supabase."""
    res = supabase.table("deputados").select("id, nome").execute()
    return res.data


def get_presencas_plenario(deputado_id):
    """
    Faz scraping da página do deputado e extrai os dados de
    Presença em Plenário para o ano corrente.
    Retorna dict com presencas, ausencias_justificadas,
    ausencias_nao_justificadas e total_sessoes.
    """
    url = f"{BASE_URL_CAMARA}/{deputado_id}"
    r = requests.get(url, params={"ano": ANO_ATUAL}, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    def extrair_valor(label):
        """
        Localiza o <li> que contém o texto do label e extrai
        o número inteiro que vem antes de 'dias' ou 'indisponível'.
        Retorna 0 se não encontrado ou indisponível.
        """
        for li in soup.find_all("li"):
            texto = li.get_text(separator=" ", strip=True)
            if label in texto:
                partes = texto.replace(label, "").strip().split()
                if partes and partes[0].isdigit():
                    return int(partes[0])
        return 0

    presencas = extrair_valor("Presenças na Câmara")
    ausencias_justificadas = extrair_valor("Ausências justificadas")
    ausencias_nao_justificadas = extrair_valor("Ausências não justificadas")
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
    Executada em paralelo para cada deputado.
    Coleta presenças e persiste nas tabelas presencas_deputados
    e metricas_deputados.
    """
    dep_id = dep["id"]
    presencas = get_presencas_plenario(dep_id)

    supabase.table("presencas_deputados").upsert(
        {
            "id_deputado": dep_id,
            "ano": ANO_ATUAL,
            "total_sessoes": presencas["total_sessoes"],
            "sessoes_presentes": presencas["presencas"],
            "faltas_justificadas": presencas["ausencias_justificadas"],
            "faltas_nao_justificadas": presencas["ausencias_nao_justificadas"],
        },
        on_conflict="id_deputado,ano",
    ).execute()

    supabase.table("metricas_deputados").upsert(
        {
            "deputado_id": dep_id,
            "data_referencia": hoje.strftime("%Y-%m-%d"),
            "total_sessoes": presencas["total_sessoes"],
            "sessoes_presentes": presencas["presencas"],
        },
        on_conflict="deputado_id,data_referencia",
    ).execute()

    return dep["nome"]


# ─── Execução principal ─────────────────────────────────────────────────────

def main():
    print(f"Iniciando coleta de presenças — {ANO_ATUAL}")

    deputados = get_deputados_do_banco()
    print(f"{len(deputados)} deputados encontrados no banco. Iniciando coleta paralela...\n")

    concluidos = 0
    erros = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(processar_deputado, dep): dep["nome"]
            for dep in deputados
        }

        for future in as_completed(futures):
            nome = futures[future]
            try:
                future.result()
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