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
        "user_id, parlamentares, despesas, emendas"
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

def notificar_parlamentares(user_id):
    favoritos = carregar_favoritos(user_id)
    atualizacoes_geradas = 0

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

        inserir_notificacao(
            user_id=user_id,
            tema="parlamentares",
            titulo=nome,
            corpo=f"{nome} teve ontem: {' e '.join(partes)}.",
            deputado_id=dep_id
        )
        atualizacoes_geradas += 1

    return atualizacoes_geradas

def notificar_despesas(user_id):
    favoritos = carregar_favoritos(user_id)
    atualizacoes_geradas = 0

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

        if not res.data or res.data[0]["total_gastos"] <= 0:
            continue

        gastos = res.data[0]["total_gastos"]

        inserir_notificacao(
            user_id=user_id,
            tema="despesas",
            titulo=nome,
            corpo=f"{nome} registrou R$ {gastos:,.2f} em despesas ontem.",
            deputado_id=dep_id
        )
        atualizacoes_geradas += 1

    return atualizacoes_geradas

def notificar_emendas(user_id):
    favoritos = carregar_favoritos(user_id)
    atualizacoes_geradas = 0

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

        total_empenhado = sum(r["valor_empenhado"] for r in res.data if r["valor_empenhado"])
        if total_empenhado <= 0:
            continue

        inserir_notificacao(
            user_id=user_id, 
            tema="emendas",
            titulo=nome,
            corpo=f"{nome} teve R$ {total_empenhado:,.2f} empenhados em emendas.",
            deputado_id=dep_id
        )
        atualizacoes_geradas += 1

    return atualizacoes_geradas

# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print(f"Iniciando envio de notificações — {hoje}")

    usuarios = carregar_usuarios_e_preferencias()
    print(f"{len(usuarios)} usuário(s) com preferências configuradas.")

    for usuario in usuarios:
        user_id = usuario["user_id"]
        tokens = get_tokens(user_id)
        
        total_atualizacoes_usuario = 0

        if usuario.get("parlamentares"):
            total_atualizacoes_usuario += notificar_parlamentares(user_id)

        if usuario.get("despesas"):
            total_atualizacoes_usuario += notificar_despesas(user_id)

        if usuario.get("emendas"):
            total_atualizacoes_usuario += notificar_emendas(user_id)

        # Dispara APENAS UM PUSH por usuário se ele teve alguma atualização
        if total_atualizacoes_usuario > 0:
            texto_atualizacao = "nova atualização" if total_atualizacoes_usuario == 1 else "novas atualizações"

            enviar_push(
                tokens,
                titulo="Radar Parlamentar",
                corpo=f"Você tem {total_atualizacoes_usuario} {texto_atualizacao} sobre os temas que acompanha.",
                data={"rota": "Notificacoes"}
            )

    print("✅ Processo finalizado.")


if __name__ == "__main__":
    main()