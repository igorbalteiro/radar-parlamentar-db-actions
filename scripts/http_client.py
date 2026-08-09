"""Cliente HTTP resiliente para as fontes públicas dos coletores."""

import random
import time
import requests


CONNECT_TIMEOUT = 10
READ_TIMEOUT = 45
MAX_TENTATIVAS = 4
STATUS_RETRY = {429, 500, 502, 503, 504}


class RespostaInvalida(requests.RequestException):
    """A fonte respondeu, mas não no formato esperado."""


def deve_tentar_novamente(erro):
    """Erros de rede e 429/5xx são transitórios; 4xx comuns não são."""
    if not isinstance(erro, requests.HTTPError):
        return True
    resposta = erro.response
    return resposta is not None and resposta.status_code in STATUS_RETRY


def get(url, *, params=None, headers=None, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        tentativas=MAX_TENTATIVAS, session=None):
    """Faz GET com timeout e backoff para falhas transitórias.

    Não faz retry para erros HTTP permanentes (por exemplo, 400 ou 404), para
    não atrasar desnecessariamente uma execução que requer correção.
    """
    ultimo_erro = None
    for tentativa in range(1, tentativas + 1):
        try:
            cliente = session or requests
            resposta = cliente.get(url, params=params, headers=headers, timeout=timeout)
            if resposta.status_code in STATUS_RETRY:
                raise requests.HTTPError(
                    f"HTTP {resposta.status_code} para {resposta.url}", response=resposta
                )
            resposta.raise_for_status()
            return resposta
        except requests.RequestException as erro:
            ultimo_erro = erro
            if tentativa == tentativas or not deve_tentar_novamente(erro):
                break
            espera = min(2 ** (tentativa - 1), 8) + random.uniform(0, 0.5)
            print(f"  tentativa {tentativa}/{tentativas} falhou ({erro}); nova tentativa em {espera:.1f}s")
            time.sleep(espera)
    raise ultimo_erro


def get_json(url, **kwargs):
    """Faz GET e valida JSON, inclusive quando um proxy retorna HTML com 200."""
    tentativas = kwargs.pop("tentativas", MAX_TENTATIVAS)
    atraso_maximo = kwargs.pop("atraso_maximo", 8)
    ultimo_erro = None
    for tentativa in range(1, tentativas + 1):
        try:
            resposta = get(url, tentativas=1, **kwargs)
            try:
                return resposta.json()
            except ValueError as erro:
                trecho = resposta.text[:160].replace("\n", " ")
                raise RespostaInvalida(
                    f"JSON inválido (HTTP {resposta.status_code}, "
                    f"Content-Type={resposta.headers.get('Content-Type')!r}, corpo={trecho!r})"
                ) from erro
        except requests.RequestException as erro:
            ultimo_erro = erro
            if tentativa == tentativas or not deve_tentar_novamente(erro):
                break
            espera = min(2 ** (tentativa - 1), atraso_maximo) + random.uniform(0, 0.5)
            print(f"  JSON/requisição inválida na tentativa {tentativa}/{tentativas} ({erro}); nova tentativa em {espera:.1f}s")
            time.sleep(espera)
    raise RespostaInvalida(f"Resposta inválida após {tentativas} tentativas: {ultimo_erro}") from ultimo_erro
