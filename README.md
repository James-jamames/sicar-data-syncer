# 🚜 SICAR Mirror & Parquet Converter

Este projeto automatiza o espelhamento mensal da base de dados do **SICAR (Sistema Nacional de Cadastro Ambiental Rural)**. Ele deleta os dados locais defasados, realiza o download dos dados atualizados de todo o Brasil e os converte para o formato **Apache Parquet**, otimizando o armazenamento e a performance de consulta.

## 🚀 Funcionalidades

* **Agendamento Inteligente:** Execução automática no dia 01 de cada mês às 00:01 via `APScheduler`.
* **Gestão de Armazenamento:** Limpeza automática do diretório de dados antes de cada novo ciclo de download.
* **Conversão Otimizada:** Transforma o conjunto de dados bruto em arquivos `.parquet`.
* **Ambiente Isolado:** Execução via Docker utilizando `uv` para gestão ultrarrápida de dependências Python.

## 🛠️ Tecnologias Utilizadas

* **Linguagem:** Python 3.12
* **Gestor de Pacotes:** [uv](https://github.com/astral-sh/uv)
* **Agendador:** APScheduler
* **Containerização:** Docker & Docker Compose
* **Formato de Saída:** Apache Parquet

## 📁 Estrutura do Projeto

```text
CAR-SCHEDULER/
├── docker/
│   └── Dockerfile          # Configuração da imagem com suporte a cron/python
├── src/
│   ├── main.py             # Ponto de entrada e lógica do agendador
│   └── scripts/
│       └── hello_world.py  # Script principal de extração e conversão
├── pyproject.toml          # Definições de dependências (uv)
└── compose.yml             # Orquestração do container

```

## ⚙️ Como Executar

### Pré-requisitos

* Docker instalado
* Docker Compose instalado

### Passo a passo

1. Clone o repositório:
```bash
git clone https://github.com/seu-usuario/car-scheduler.git
cd car-scheduler

```


2. Suba o serviço:
```bash
docker compose up -d --build

```


3. Verifique se o agendador está ativo:
```bash
docker compose logs -f

```



## ⏳ Ciclo de Execução (Cron)

O sistema está configurado para o seguinte gatilho:

* **Frequência:** Mensal
* **Dia:** 01
* **Horário:** 00:01
* **Fuso Horário:** UTC (conforme configuração do container)