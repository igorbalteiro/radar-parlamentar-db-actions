"""Importa as despesas da cota parlamentar do ano corrente para o Supabase."""

import csv
import io
import os
import zipfile
from datetime import datetime
from supabase import create_client
from http_client import get


SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

ANO_ATUAL = datetime.now().year
URL_ARQUIVO = f"https://www.camara.leg.br/cotas/Ano-{ANO_ATUAL}.csv.zip"
TAMANHO_LOTE = 500
COLUNAS_OBRIGATORIAS = {
    "ideCadastro", "numAno", "numMes", "txtDescricao", "indTipoDocumento",
    "vlrDocumento", "vlrGlosa", "vlrLiquido",
}


def texto(valor):
    """Converte campos vazios do CSV em NULL."""
    if valor is None:
        return None
    valor = valor.strip()
    return valor or None


def inteiro(valor):
    valor = texto(valor)
    return int(valor) if valor is not None else None


def decimal(valor):
    """Mantém precisão ao enviar valores numéricos ao Postgres."""
    valor = texto(valor)
    if valor is None:
        return "0"
    # A Câmara usualmente usa vírgula decimal; a forma sem vírgula já é aceita
    # pelo Postgres e não deve perder o ponto decimal.
    return valor.replace(".", "").replace(",", ".") if "," in valor else valor


def data(valor):
    valor = texto(valor)
    return valor[:10] if valor else None


def baixar_arquivo():
    """Baixa e valida o ZIP de despesas publicado pela Câmara."""
    resposta = get(
        URL_ARQUIVO,
        headers={"Accept": "application/zip, application/octet-stream, */*"},
        timeout=(15, 300),
        tentativas=6,
    )
    conteudo = resposta.content
    if not zipfile.is_zipfile(io.BytesIO(conteudo)):
        raise RuntimeError("A Câmara não retornou um arquivo ZIP válido de despesas.")
    return conteudo


def localizar_csv(pacote):
    """Localiza o CSV dentro do ZIP, independente do nome interno."""
    for nome in pacote.namelist():
        if nome.lower().endswith(".csv"):
            return nome
    raise RuntimeError("Nenhum arquivo CSV foi encontrado no ZIP de despesas.")


def abrir_csv(pacote, nome_csv):
    """Abre CSV UTF-8 ou ISO-8859-1, codificações usadas pela Câmara."""
    with pacote.open(nome_csv) as arquivo:
        amostra = arquivo.read(8192)
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            amostra.decode(encoding)
        except UnicodeDecodeError:
            continue
        arquivo = pacote.open(nome_csv)
        return io.TextIOWrapper(arquivo, encoding=encoding, newline="")
    raise RuntimeError("Não foi possível identificar a codificação do CSV de despesas.")


def iterar_despesas(conteudo_zip):
    """Lê o CSV em streaming e devolve registros compatíveis com a tabela destino."""
    with zipfile.ZipFile(io.BytesIO(conteudo_zip)) as pacote:
        nome_csv = localizar_csv(pacote)
        with abrir_csv(pacote, nome_csv) as texto_csv:
            leitor = csv.DictReader(texto_csv, delimiter=";", quotechar='"')
            faltantes = COLUNAS_OBRIGATORIAS - set(leitor.fieldnames or [])
            if faltantes:
                raise RuntimeError(
                    f"CSV sem coluna(s) obrigatória(s): {', '.join(sorted(faltantes))}"
                )

            for linha in leitor:
                deputado_id = inteiro(linha["ideCadastro"])
                if deputado_id is None:
                    # Registros de órgãos/lideranças não possuem cadastro de deputado.
                    continue

                ano = inteiro(linha["numAno"])
                if ano != ANO_ATUAL:
                    continue

                tipo_despesa = texto(linha["txtDescricao"])
                if tipo_despesa is None:
                    continue

                yield {
                    "deputado_id": deputado_id,
                    "nome_parlamentar": texto(linha.get("txNomeParlamentar")),
                    "ano": ano,
                    "mes": inteiro(linha["numMes"]),
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


def main():
    print(f"Baixando despesas da cota parlamentar — exercício {ANO_ATUAL}...")
    conteudo_zip = baixar_arquivo()
    print(f"ZIP recebido ({len(conteudo_zip) / 1024 / 1024:.1f} MB). Validando CSV...")

    # Abre o arquivo e valida o cabeçalho antes de remover dados já existentes.
    iterador = iterar_despesas(conteudo_zip)
    try:
        primeiro_registro = next(iterador)
    except StopIteration:
        raise RuntimeError("O CSV de despesas não possui registros de deputados para o ano atual.")

    print(f"Removendo despesas já importadas para {ANO_ATUAL}...")
    supabase.table("despesas_deputados").delete().eq("ano", ANO_ATUAL).execute()

    lote = [primeiro_registro]
    total = 0
    for despesa in iterador:
        lote.append(despesa)
        if len(lote) == TAMANHO_LOTE:
            salvar_lote(lote)
            total += len(lote)
            print(f"  {total} despesas salvas...")
            lote = []

    if lote:
        salvar_lote(lote)
        total += len(lote)

    print(f"✅ Coleta finalizada — {total} despesas armazenadas.")


if __name__ == "__main__":
    main()
