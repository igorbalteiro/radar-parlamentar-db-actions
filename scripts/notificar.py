import os
import requests
from datetime import datetime, timedelta
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
hoje = datetime.today().date()
ontem = hoje - timedelta(days=1)


# ─── Helpers ────────────────────────────────────────────────────────────────

def get_tokens(user_id):
    res = supabase.table("push_tokens") \
        .select("token") \
        .eq("user_id", user_id) \
        .eq("ativa", True) \
        .execute()
    return [r["token"] for r in res.data]


def ja_notificou(user_id, tema, deputado_id, data):
    query = supabase.table("notificacoes") \
        .select("id") \
        .eq("user_id", user_id) \
        .eq("tema", tema) \
        .gte("criado_em", str(data))

    if deputado_id:
        query = query.eq("deputado_id", deputado_id)

    return len(query.execute().data) > 0


def inserir_notificacao(user_id, tema, titulo, corpo, deputado_id=None):
    supabase.table("notificacoes").insert({
        "user_id": user_id,
        "tema": tema,
        "titulo": titulo,
        "corpo": corpo,
        "deputado_id": deputado_id,
        "lida": False,
    }).execute()


def enviar_push(tokens, titulo, corpo, data={}):
    if not tokens:
        return
    mensagens = [
        {"to": token, "title": titulo, "body": corpo, "data": data}
        for token in tokens
    ]
    requests.post(EXPO_PUSH_URL, json=mensagens, timeout=10)


def carregar_usuarios_e_preferencias():
    res = supabase.table("notificacoes_preferencias").select(
        "user_id, parlamentares, despesas, emendas, votacoes, proposicoes"
    ).execute()
    return res.data


def carregar_favoritos(user_id):
    """
    Busca os parlamentares favoritados pelo usuário.
    A tabela favoritos usa item_id (TEXT) como ID do deputado
    e dados_snapshot (JSONB) para armazenar o nome.
    """
    res = supabase.table("favoritos") \
        .select("item_id, dados_snapshot") \
        .eq("user_id", user_id) \
        .eq("tipo", "parlamentar") \
        .execute()

    return [
        {
            "deputado_id": int(fav["item_id"]),
            "deputados": {
                "nome": fav["dados_snapshot"].get("nome", "")
            }
        }
        for fav in res.data
        if fav.get("item_id")
    ]


# ─── Notificadores por tema ──────────────────────────────────────────────────

def notificar_parlamentares(user_id, tokens):
    favoritos = carregar_favoritos(user_id)
    deputados_com_atualizacao = []

    for fav in favoritos:
        dep_id = fav["deputado_id"]
        nome = fav["deputados"]["nome"]

        if ja_notificou(user_id, "parlamentares", dep_id, ontem):
            continue

        res = supabase.table("metricas_deputados") \
            .select("qtd_discursos, qtd_proposicoes") \
            .eq("deputado_id", dep_id) \
            .eq("data_referencia", str(ontem)) \
            .execute()

        if not res.data:
            continue

        metricas = res.data[0]
        discursos = metricas["qtd_discursos"]
        proposicoes = metricas["qtd_proposicoes"]

        if discursos == 0 and proposicoes == 0:
            continue

        partes = []
        if discursos > 0:
            partes.append(f"{discursos} discurso(s)")
        if proposicoes > 0:
            partes.append(f"{proposicoes} proposição(ões)")

        deputados_com_atualizacao.append({
            "id": dep_id,
            "nome": nome,
            "resumo": " e ".join(partes),
        })

        inserir_notificacao(
            user_id, "parlamentares",
            f"Atualização — {nome}",
            f"{nome} teve ontem: {' e '.join(partes)}.",
            dep_id
        )

    if not deputados_com_atualizacao:
        return

    total = len(deputados_com_atualizacao)
    sobrenomes = [d["nome"].split()[-1] for d in deputados_com_atualizacao]

    if total == 1:
        corpo = f"{sobrenomes[0]} teve atualizações ontem."
    elif total == 2:
        corpo = f"{sobrenomes[0]} e {sobrenomes[1]} tiveram atualizações ontem."
    else:
        corpo = f"{sobrenomes[0]}, {sobrenomes[1]} e mais {total - 2} tiveram atualizações ontem."

    enviar_push(
        tokens,
        titulo=f"Parlamentares — {total} atualização(ões)",
        corpo=corpo,
        data={"tema": "parlamentares"}
    )

def notificar_despesas(user_id, tokens):
    favoritos = carregar_favoritos(user_id)
    deputados_com_despesa = []

    for fav in favoritos:
        dep_id = fav["deputado_id"]
        nome = fav["deputados"]["nome"]

        if ja_notificou(user_id, "despesas", dep_id, ontem):
            continue

        res = supabase.table("metricas_deputados") \
            .select("total_gastos") \
            .eq("deputado_id", dep_id) \
            .eq("data_referencia", str(ontem)) \
            .execute()

        if not res.data:
            continue

        gastos = res.data[0]["total_gastos"]
        if gastos <= 0:
            continue

        deputados_com_despesa.append({"nome": nome, "gastos": gastos, "id": dep_id})

        inserir_notificacao(
            user_id, "despesas",
            f"Nova despesa — {nome}",
            f"{nome} registrou R$ {gastos:,.2f} em despesas ontem.",
            dep_id
        )

    if not deputados_com_despesa:
        return

    # Monta o texto agrupado para o push
    total = len(deputados_com_despesa)
    nomes = [d["nome"].split()[-1] for d in deputados_com_despesa]  # só sobrenome

    if total == 1:
        corpo = f"{nomes[0]} registrou novas despesas ontem."
    elif total == 2:
        corpo = f"{nomes[0]} e {nomes[1]} registraram novas despesas ontem."
    else:
        corpo = f"{nomes[0]}, {nomes[1]} e mais {total - 2} registraram novas despesas ontem."

    # Um único push para o tema inteiro
    enviar_push(
        tokens,
        titulo=f"Despesas — {total} atualização(ões)",
        corpo=corpo,
        data={"tema": "despesas"}
    )

def notificar_emendas(user_id, tokens):
    favoritos = carregar_favoritos(user_id)
    deputados_com_emenda = []

    for fav in favoritos:
        dep_id = fav["deputado_id"]
        nome = fav["deputados"]["nome"]

        if ja_notificou(user_id, "emendas", dep_id, ontem):
            continue

        res = supabase.table("emendas_parlamentares") \
            .select("valor_empenhado") \
            .eq("deputado_id", dep_id) \
            .gte("coletado_em", str(ontem)) \
            .execute()

        if not res.data:
            continue

        total_empenhado = sum(
            r["valor_empenhado"] for r in res.data
            if r["valor_empenhado"] is not None
        )

        if total_empenhado <= 0:
            continue

        deputados_com_emenda.append({
            "id": dep_id,
            "nome": nome,
            "total": total_empenhado,
        })

        inserir_notificacao(
            user_id, "emendas",
            f"Nova emenda — {nome}",
            f"{nome} teve R$ {total_empenhado:,.2f} empenhados em emendas.",
            dep_id
        )

    if not deputados_com_emenda:
        return

    total = len(deputados_com_emenda)
    sobrenomes = [d["nome"].split()[-1] for d in deputados_com_emenda]

    if total == 1:
        corpo = f"{sobrenomes[0]} teve emendas empenhadas."
    elif total == 2:
        corpo = f"{sobrenomes[0]} e {sobrenomes[1]} tiveram emendas empenhadas."
    else:
        corpo = f"{sobrenomes[0]}, {sobrenomes[1]} e mais {total - 2} tiveram emendas empenhadas."

    enviar_push(
        tokens,
        titulo=f"Emendas — {total} atualização(ões)",
        corpo=corpo,
        data={"tema": "emendas"}
    )

def notificar_votacoes(user_id, tokens):
    if ja_notificou(user_id, "votacoes", None, ontem):
        return

    res = supabase.table("metricas_deputados") \
        .select("deputado_id") \
        .eq("data_referencia", str(ontem)) \
        .gt("qtd_discursos", 0) \
        .limit(1) \
        .execute()

    if not res.data:
        return

    titulo = "Novas votações no Plenário"
    corpo = "Houve votações nominais ontem na Câmara. Veja como seus parlamentares votaram."

    inserir_notificacao(user_id, "votacoes", titulo, corpo)
    enviar_push(tokens, titulo, corpo, {"tema": "votacoes"})

def notificar_proposicoes(user_id, tokens):
    favoritos = carregar_favoritos(user_id)
    deputados_com_proposicao = []

    for fav in favoritos:
        dep_id = fav["deputado_id"]
        nome = fav["deputados"]["nome"]

        if ja_notificou(user_id, "proposicoes", dep_id, ontem):
            continue

        res = supabase.table("metricas_deputados") \
            .select("qtd_proposicoes") \
            .eq("deputado_id", dep_id) \
            .eq("data_referencia", str(ontem)) \
            .execute()

        if not res.data:
            continue

        qtd = res.data[0]["qtd_proposicoes"]
        if qtd == 0:
            continue

        deputados_com_proposicao.append({
            "id": dep_id,
            "nome": nome,
            "qtd": qtd,
        })

        inserir_notificacao(
            user_id, "proposicoes",
            f"Nova proposição — {nome}",
            f"{nome} apresentou {qtd} proposição(ões) ontem.",
            dep_id
        )

    if not deputados_com_proposicao:
        return

    total = len(deputados_com_proposicao)
    sobrenomes = [d["nome"].split()[-1] for d in deputados_com_proposicao]

    if total == 1:
        corpo = f"{sobrenomes[0]} apresentou proposição(ões) ontem."
    elif total == 2:
        corpo = f"{sobrenomes[0]} e {sobrenomes[1]} apresentaram proposições ontem."
    else:
        corpo = f"{sobrenomes[0]}, {sobrenomes[1]} e mais {total - 2} apresentaram proposições ontem."

    enviar_push(
        tokens,
        titulo=f"Proposições — {total} atualização(ões)",
        corpo=corpo,
        data={"tema": "proposicoes"}
    )

# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print(f"Iniciando envio de notificações — {hoje}")

    usuarios = carregar_usuarios_e_preferencias()
    print(f"{len(usuarios)} usuário(s) com preferências configuradas.")

    for usuario in usuarios:
        user_id = usuario["user_id"]
        tokens = get_tokens(user_id)

        if usuario.get("parlamentares"):
            notificar_parlamentares(user_id, tokens)

        if usuario.get("despesas"):
            notificar_despesas(user_id, tokens)

        if usuario.get("emendas"):
            notificar_emendas(user_id, tokens)

        if usuario.get("votacoes"):
            notificar_votacoes(user_id, tokens)

        if usuario.get("proposicoes"):
            notificar_proposicoes(user_id, tokens)

    print("✅ Notificações enviadas.")


if __name__ == "__main__":
    main()