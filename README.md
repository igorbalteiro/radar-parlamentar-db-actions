# Radar Parlamentar — Pipeline de Dados

Repositório responsável pela coleta, processamento e armazenamento dos dados parlamentares utilizados pelo app [Radar Parlamentar](https://github.com/seu-usuario/radar-parlamentar). Os scripts rodam automaticamente via GitHub Actions e alimentam o banco de dados no Supabase.

---

## O que esse repositório faz

Coleta diariamente dados públicos sobre os deputados federais brasileiros e os armazena no Supabase para consumo pelo app mobile. As informações coletadas incluem gastos com a cota parlamentar, discursos, proposições, emendas parlamentares, presenças em plenário e envio de notificações push para os usuários.

---

## Estrutura

```
├── scripts/
│   ├── coletar_deputados.py     # Cadastro e status dos deputados
│   ├── coletar_metricas.py      # Gastos, discursos e proposições
│   ├── coletar_emendas.py       # Emendas parlamentares
│   ├── coletar_presencas.py     # Presenças em plenário
│   └── notificar.py             # Envio de notificações push
├── .github/
│   └── workflows/
│       └── cron.yml             # Agendamento e execução dos scripts
└── requirements.txt
```

---

## Scripts

### `coletar_deputados.py`
Busca todos os deputados ativos da legislatura 57 via API da Câmara dos Deputados e atualiza a tabela `deputados` no Supabase com nome, partido, UF, foto e status atual do mandato.

**Fonte:** [dadosabertos.camara.leg.br](https://dadosabertos.camara.leg.br/api/v2/deputados)

---

### `coletar_metricas.py`
Coleta as métricas dos deputados nos últimos 30 dias: total de gastos com a cota parlamentar, quantidade de discursos e quantidade de proposições apresentadas. Os dados são processados em paralelo usando `ThreadPoolExecutor` para otimizar o tempo de execução.

**Fonte:** [dadosabertos.camara.leg.br](https://dadosabertos.camara.leg.br/api/v2)

---

### `coletar_emendas.py`
Coleta todas as emendas parlamentares do ano corrente no Portal da Transparência, vincula cada emenda ao deputado correspondente por normalização de nome e armazena os valores empenhados, liquidados e pagos. Também coleta os documentos relacionados a cada emenda.

**Fonte:** [portaldatransparencia.gov.br](https://portaldatransparencia.gov.br/emendas)

---

### `coletar_presencas.py`
Faz scraping da página de cada deputado no site da Câmara para extrair os dados de presença em plenário do ano corrente: total de sessões, presenças, faltas justificadas e faltas não justificadas.

**Fonte:** [camara.leg.br/deputados](https://www.camara.leg.br/deputados)

---

### `notificar.py`
Verifica quais deputados favoritados pelos usuários tiveram atualizações desde a última coleta (novos gastos, discursos, proposições ou emendas) e envia um único push notification consolidado por usuário via Expo Push Notifications. Cada atualização é também registrada individualmente na tabela `notificacoes` para exibição no app.

**Destino:** [Expo Push Notifications](https://docs.expo.dev/push-notifications/overview)

---

## Agendamento

Os scripts são executados automaticamente via GitHub Actions com a seguinte frequência:

| Frequência | Horário | Scripts |
|---|---|---|
| Diária | 06h00 UTC | `coletar_metricas.py`, `notificar.py` |
| Semanal (sábado) | 08h00 UTC | `coletar_deputados.py`, `coletar_presencas.py` |
| Semanal (sábado) | 08h00 UTC | `coletar_emendas.py` |

Todos os scripts também podem ser executados manualmente pela aba **Actions** do repositório, selecionando o script desejado no campo **script_selecionado**.

---

## Como executar localmente

**Pré-requisitos**
- Python 3.11+
- Credenciais do Supabase (`service_role` key)

**Instalação**

```bash
pip install -r requirements.txt
```

**Execução**

```bash
export SUPABASE_URL=https://xxxx.supabase.co
export SUPABASE_KEY=sua_service_role_key

python scripts/coletar_deputados.py
python scripts/coletar_metricas.py
python scripts/coletar_emendas.py
python scripts/coletar_presencas.py
python scripts/notificar.py
```

---

## Configuração

### Secrets do GitHub

Configure os seguintes secrets em **Settings → Secrets → Actions** do repositório:

| Secret | Descrição |
|---|---|
| `SUPABASE_URL` | URL do projeto Supabase (Project URL) |
| `SUPABASE_KEY` | Chave `service_role` do Supabase |

Ambos podem ser encontrados em **Project Settings → Data API** no painel do Supabase.

---

## Banco de dados

Os scripts alimentam as seguintes tabelas no Supabase:

| Tabela | Descrição |
|---|---|
| `deputados` | Cadastro dos deputados federais |
| `metricas_deputados` | Gastos, discursos e proposições por deputado por dia |
| `emendas_parlamentares` | Emendas do ano corrente com valores financeiros |
| `emendas_detalhes` | Documentos relacionados a cada emenda |
| `presencas_deputados` | Presenças e faltas em plenário por ano |
| `notificacoes` | Notificações geradas para cada usuário |
| `push_tokens` | Tokens de dispositivo para push notifications |
| `notificacoes_preferencias` | Preferências de notificação por usuário |

---

## Fontes de dados

| Fonte | Dados |
|---|---|
| [API da Câmara dos Deputados](https://dadosabertos.camara.leg.br/api/v2) | Deputados, gastos, discursos, proposições |
| [Portal da Transparência](https://portaldatransparencia.gov.br) | Emendas parlamentares |
| [Site da Câmara](https://www.camara.leg.br) | Presenças em plenário (scraping) |
| [Congresso em Foco](https://radar.congressoemfoco.com.br) | Dados históricos de assiduidade (2023–2025) |