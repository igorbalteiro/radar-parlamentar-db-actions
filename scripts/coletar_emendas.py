"""Coleta emendas pelo arquivo oficial de dados abertos do Portal da Transparência."""

import csv
import io
import os
import re
import unicodedata
import zipfile
from datetime import datetime

import requests
from supabase import create_client

from http_client import get


SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

URL_DOWNLOAD = "https://portaldatransparencia.gov.br/download-de-dados/emendas-parlamentares/UNICO"
ARQUIVO_EMENDAS = "EmendasParlamentares.csv"
TAMANHO_LOTE = 500
ENCODINGS_CSV = ("utf-8-sig", "cp1252", "latin-1")
ANO_ATUAL = datetime.now().year

HEADERS = {
    "Accept": "application/zip, application/octet-stream, */*",
    "User-Agent": "RadarParlamentar/1.0 (+https://github.com/radarparlamentar/radar-parlamentar-db-actions)",
}


def parse_valor(valor):
    """Converte valores no formato brasileiro do CSV para float."""
    if not valor or valor.strip() == "Sem informação":
        return None
    try:
        return float(valor.strip().replace(".", "").replace(",", "."))
    except (ValueError, AttributeError):
        return None


def texto(valor):
    """Normaliza campos textuais vazios e o marcador do arquivo de origem."""
    if valor is None:
        return None
    valor = valor.strip()
    return None if not valor or valor == "Sem informação" else valor


def classificacao(codigo, nome):
    """Armazena código e descrição em uma única coluna do schema atual."""
    codigo = texto(codigo)
    nome = texto(nome)
    return f"{codigo} - {nome}" if codigo and nome else codigo or nome


def normalizar_nome(nome):
    """Normaliza nomes para associar o autor ao deputado cadastrado."""
    if not nome:
        return ""
    nome = unicodedata.normalize("NFD", nome)
    nome = "".join(c for c in nome if unicodedata.category(c) != "Mn")
    return " ".join(re.sub(r"\s+", " ", nome).strip().lower().split())


def carregar_deputados():
    """Cria um índice de deputados para preencher a chave estrangeira quando possível."""
    resposta = supabase.table("deputados").select("id, nome").execute()
    return {normalizar_nome(dep["nome"]): dep["id"] for dep in resposta.data}


def baixar_arquivo():
    """Baixa e valida o ZIP disponibilizado pelo Portal da Transparência."""
    with requests.Session() as session:
        resposta = get(
            URL_DOWNLOAD,
            headers=HEADERS,
            timeout=(15, 300),
            tentativas=6,
            session=session,
        )

    conteudo = resposta.content
    if not zipfile.is_zipfile(io.BytesIO(conteudo)):
        content_type = resposta.headers.get("Content-Type", "desconhecido")
        trecho = resposta.text[:160].replace("\n", " ")
        raise RuntimeError(
            "O Portal não retornou um ZIP válido "
            f"(HTTP {resposta.status_code}, Content-Type={content_type!r}, corpo={trecho!r})"
        )
    return conteudo


def localizar_csv(pacote):
    """Localiza o CSV principal, mesmo que o ZIP o coloque em uma pasta."""
    for nome in pacote.namelist():
        if nome.rsplit("/", 1)[-1].lower() == ARQUIVO_EMENDAS.lower():
            return nome
    raise RuntimeError(f"Arquivo {ARQUIVO_EMENDAS!r} não encontrado no ZIP.")


def detectar_encoding(pacote, nome_csv):
    """Identifica UTF-8 ou a codificação legada usada nos CSVs do Portal."""
    with pacote.open(nome_csv) as arquivo:
        amostra = arquivo.read(8192)

    for encoding in ENCODINGS_CSV:
        try:
            cabecalho = amostra.decode(encoding)
        except UnicodeDecodeError:
            continue
        if "Código da Emenda" in cabecalho:
            return encoding
    raise RuntimeError("Não foi possível identificar a codificação do CSV de emendas.")


def iterar_emendas(conteudo_zip, deputados_index):
    """Lê o CSV em streaming e o transforma no formato da tabela de destino."""
    with zipfile.ZipFile(io.BytesIO(conteudo_zip)) as pacote:
        nome_csv = localizar_csv(pacote)
        encoding = detectar_encoding(pacote, nome_csv)

        with pacote.open(nome_csv) as arquivo:
            texto_csv = io.TextIOWrapper(arquivo, encoding=encoding, newline="")
            leitor = csv.DictReader(texto_csv, delimiter=";", quotechar='"')
            obrigatorios = {"Código da Emenda", "Ano da Emenda", "Valor Empenhado"}
            faltantes = obrigatorios - set(leitor.fieldnames or [])
            if faltantes:
                raise RuntimeError(f"CSV sem coluna(s) obrigatória(s): {', '.join(sorted(faltantes))}")

            for linha in leitor:
                ano = int(linha["Ano da Emenda"])
                if ano != ANO_ATUAL:
                    continue

                nome_autor = texto(linha["Nome do Autor da Emenda"])
                yield {
                    "codigo_emenda": texto(linha["Código da Emenda"]),
                    "ano": ano,
                    "tipo_emenda": texto(linha["Tipo de Emenda"]),
                    "autor": texto(linha["Código do Autor da Emenda"]),
                    "nome_autor": nome_autor,
                    "numero_emenda": texto(linha["Número da emenda"]),
                    "localidade_do_gasto": texto(linha["Localidade de aplicação do recurso"]),
                    "funcao": classificacao(linha["Código Função"], linha["Nome Função"]),
                    "subfuncao": classificacao(linha["Código Subfunção"], linha["Nome Subfunção"]),
                    "programa": classificacao(linha["Código Programa"], linha["Nome Programa"]),
                    "acao": classificacao(linha["Código Ação"], linha["Nome Ação"]),
                    "plano_orcamentario": classificacao(
                        linha["Código Plano Orçamentário"],
                        linha["Nome Plano Orçamentário"],
                    ),
                    "valor_empenhado": parse_valor(linha["Valor Empenhado"]),
                    "valor_liquidado": parse_valor(linha["Valor Liquidado"]),
                    "valor_pago": parse_valor(linha["Valor Pago"]),
                    "valor_resto_inscrito": parse_valor(linha["Valor Restos A Pagar Inscritos"]),
                    "valor_resto_cancelado": parse_valor(linha["Valor Restos A Pagar Cancelados"]),
                    "valor_resto_pago": parse_valor(linha["Valor Restos A Pagar Pagos"]),
                    "deputado_id": deputados_index.get(normalizar_nome(nome_autor)),
                    "link_detalhamento": None,
                    "coletado_em": datetime.now().isoformat(),
                }


def salvar_lote(lote):
    supabase.table("emendas_parlamentares").upsert(
        lote,
        on_conflict="codigo_emenda,ano,tipo_emenda,valor_empenhado",
    ).execute()


def main():
    print(f"Baixando arquivo oficial de emendas — exercício {ANO_ATUAL}...")
    conteudo_zip = baixar_arquivo()
    print(f"ZIP recebido ({len(conteudo_zip) / 1024 / 1024:.1f} MB).")

    deputados_index = carregar_deputados()
    lote = []
    total = 0

    for emenda in iterar_emendas(conteudo_zip, deputados_index):
        lote.append(emenda)
        if len(lote) == TAMANHO_LOTE:
            salvar_lote(lote)
            total += len(lote)
            print(f"  {total} emendas salvas...")
            lote = []

    if lote:
        salvar_lote(lote)
        total += len(lote)

    print(f"✅ Coleta finalizada — {total} emendas armazenadas.")


if __name__ == "__main__":
    main()
