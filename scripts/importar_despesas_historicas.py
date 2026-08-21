"""Importa localmente arquivos CSV ou ZIP de despesas históricas da Câmara."""

import argparse
import csv
import io
import os
import sys
import zipfile
from contextlib import contextmanager
from pathlib import Path

from supabase import create_client


SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TAMANHO_LOTE = 500
TAMANHO_LOTE_EXCLUSAO = 500
COLUNAS_OBRIGATORIAS = {
    "ideCadastro", "numAno", "numMes", "txtDescricao", "indTipoDocumento",
    "vlrDocumento", "vlrGlosa", "vlrLiquido",
}


def texto(valor):
    if valor is None:
        return None
    valor = valor.strip()
    return valor or None


def inteiro(valor):
    valor = texto(valor)
    return int(valor) if valor is not None else None


def decimal(valor):
    """Converte formatos brasileiro e internacional sem perder precisão."""
    valor = texto(valor)
    if valor is None:
        return "0"
    return valor.replace(".", "").replace(",", ".") if "," in valor else valor


def data(valor):
    valor = texto(valor)
    return valor[:10] if valor else None


def encoding_do_csv(amostra):
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            cabecalho = amostra.decode(encoding)
        except UnicodeDecodeError:
            continue
        if "ideCadastro" in cabecalho:
            return encoding
    raise RuntimeError("Não foi possível identificar a codificação ou o cabeçalho do CSV.")


@contextmanager
def abrir_csv(caminho):
    """Abre um CSV diretamente ou o único CSV contido em um ZIP local."""
    if caminho.suffix.lower() == ".zip":
        with zipfile.ZipFile(caminho) as pacote:
            csvs = [nome for nome in pacote.namelist() if nome.lower().endswith(".csv")]
            if len(csvs) != 1:
                raise RuntimeError(
                    f"{caminho}: esperado exatamente um CSV no ZIP; encontrados {len(csvs)}."
                )
            nome_csv = csvs[0]
            with pacote.open(nome_csv) as arquivo:
                amostra = arquivo.read(8192)
            with pacote.open(nome_csv) as arquivo:
                yield io.TextIOWrapper(arquivo, encoding=encoding_do_csv(amostra), newline="")
        return

    with caminho.open("rb") as arquivo:
        amostra = arquivo.read(8192)
    with caminho.open("rb") as arquivo:
        yield io.TextIOWrapper(arquivo, encoding=encoding_do_csv(amostra), newline="")


def leitor_do_arquivo(caminho):
    with abrir_csv(caminho) as arquivo:
        leitor = csv.DictReader(arquivo, delimiter=";", quotechar='"')
        faltantes = COLUNAS_OBRIGATORIAS - set(leitor.fieldnames or [])
        if faltantes:
            raise RuntimeError(
                f"{caminho}: CSV sem coluna(s) obrigatória(s): {', '.join(sorted(faltantes))}"
            )
        yield leitor


def anos_no_arquivo(caminho):
    """Valida o arquivo e retorna os anos presentes antes de alterar o banco."""
    for leitor in leitor_do_arquivo(caminho):
        anos = {inteiro(linha["numAno"]) for linha in leitor if inteiro(linha["numAno"])}
    if not anos:
        raise RuntimeError(f"{caminho}: nenhum ano de despesa encontrado.")
    return anos


def iterar_despesas(caminho):
    for leitor in leitor_do_arquivo(caminho):
        for linha in leitor:
            deputado_id = inteiro(linha["ideCadastro"])
            ano = inteiro(linha["numAno"])
            mes = inteiro(linha["numMes"])
            tipo_despesa = texto(linha["txtDescricao"])
            if deputado_id is None or ano is None or mes is None or tipo_despesa is None:
                continue

            yield {
                "deputado_id": deputado_id,
                "nome_parlamentar": texto(linha.get("txNomeParlamentar")),
                "ano": ano,
                "mes": mes,
                "cnpj_cpf_fornecedor": texto(linha.get("txtCNPJCPF")),
                "cod_documento": texto(linha.get("ideDocumento")),
                "cod_lote": inteiro(linha.get("numLote")),
                "cod_tipo_documento": inteiro(linha["indTipoDocumento"]),
                "data_documento": data(linha.get("datEmissao")),
                "nome_fornecedor": texto(linha.get("txtFornecedor")),
                "num_documento": texto(linha.get("txtNumero")),
                "num_ressarcimento": texto(linha.get("numRessarcimento")),
                "parcela": inteiro(linha.get("numParcela")),
                "tipo_despesa": tipo_despesa,
                "url_documento": texto(linha.get("urlDocumento")),
                "valor_documento": decimal(linha["vlrDocumento"]),
                "valor_glosa": decimal(linha["vlrGlosa"]),
                "valor_liquido": decimal(linha["vlrLiquido"]),
            }


def salvar_lote(lote):
    supabase.table("despesas_deputados").insert(lote).execute()


def remover_despesas_do_ano(ano):
    """Remove um ano em lotes para não exceder o statement timeout do Supabase."""
    total = 0
    while True:
        resposta = (
            supabase.table("despesas_deputados")
            .select("id")
            .eq("ano", ano)
            .limit(TAMANHO_LOTE_EXCLUSAO)
            .execute()
        )
        ids = [registro["id"] for registro in resposta.data]
        if not ids:
            return total

        supabase.table("despesas_deputados").delete().in_("id", ids).execute()
        total += len(ids)
        print(f"  {total} despesas removidas de {ano}...", flush=True)


def importar(caminhos):
    total = 0
    for caminho in caminhos:
        print(f"Importando {caminho.name}...", flush=True)
        lote = []
        for despesa in iterar_despesas(caminho):
            lote.append(despesa)
            if len(lote) == TAMANHO_LOTE:
                salvar_lote(lote)
                total += len(lote)
                print(f"  {total} despesas salvas...", flush=True)
                lote = []
        if lote:
            salvar_lote(lote)
            total += len(lote)
    return total


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("arquivos", nargs="+", type=Path, help="Arquivos CSV ou ZIP locais.")
    argumentos = parser.parse_args()

    caminhos = []
    for caminho in argumentos.arquivos:
        if not caminho.is_file():
            parser.error(f"arquivo não encontrado: {caminho}")
        if caminho.suffix.lower() not in {".csv", ".zip"}:
            parser.error(f"formato não suportado: {caminho} (use .csv ou .zip)")
        caminhos.append(caminho)

    anos = set().union(*(anos_no_arquivo(caminho) for caminho in caminhos))
    print(f"Arquivos validados. Anos encontrados: {', '.join(map(str, sorted(anos)))}.")

    # A tabela não possui chave única natural para upsert. Substituir os anos
    # presentes no arquivo garante uma carga idempotente sem criar duplicatas.
    for ano in sorted(anos):
        print(f"Removendo despesas existentes de {ano}...", flush=True)
        removidas = remover_despesas_do_ano(ano)
        print(f"  {removidas} despesas removidas de {ano}.", flush=True)

    total = importar(caminhos)
    print(f"✅ Importação finalizada — {total} despesas armazenadas.")


if __name__ == "__main__":
    main()
