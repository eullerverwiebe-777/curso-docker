"""
ETL de vendas — o fio condutor do curso.

Le o CSV, limpa a sujeira, e grava no Postgres.

Aula 3: o Postgres sobe num container.
Aula 4: este script vira uma imagem.
Aula 6: ele acha o banco pelo NOME do container, nao por localhost.
Aula 7: o Compose sobe os dois juntos.
"""

import os
import sys
import time

import pandas as pd
from sqlalchemy import create_engine, text


# ─────────────────────────────────────────────────────────────
# Configuracao — tudo vem de variavel de ambiente.
#
# Repare no default do DB_HOST: "localhost".
# Na aula 6 voce vai ver esse default FALHAR dentro do container,
# e vai trocar por DB_HOST=postgres. Isso e proposital.
# ─────────────────────────────────────────────────────────────

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "senha")

CSV = os.getenv("CSV_PATH", "dados/vendas.csv")
TABELA = "vendas"


def conectar(tentativas=10, espera=3):
    """
    Tenta conectar no banco, com paciencia.

    Por que isso existe: o Postgres demora alguns segundos pra ficar
    pronto depois que o container sobe. Sem essa espera, o ETL morre
    com "connection refused" mesmo estando tudo certo.

    Na aula 7 o healthcheck do Compose resolve isso de um jeito
    muito mais elegante — e voce vai poder simplificar esta funcao.
    """
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    for i in range(1, tentativas + 1):
        try:
            engine = create_engine(url)
            with engine.connect() as c:
                c.execute(text("SELECT 1"))
            print(f"  Conectado em {DB_HOST}:{DB_PORT}/{DB_NAME}")
            return engine
        except Exception as e:
            if i == tentativas:
                print(f"\n  Nao consegui conectar em {DB_HOST}:{DB_PORT}")
                print(f"  Ultimo erro: {type(e).__name__}: {e}\n")
                print("  Dicas:")
                print("    - O container do Postgres esta rodando? (docker ps)")
                print("    - O DB_HOST esta certo? Dentro de um container,")
                print("      'localhost' e o proprio container — nao o banco.")
                print("      Use o NOME do container/servico.\n")
                sys.exit(1)
            print(f"  Tentativa {i}/{tentativas} falhou. Esperando {espera}s...")
            time.sleep(espera)


def limpar(df):
    """
    Limpa a sujeira do CSV. Isso e trabalho de dados de verdade —
    o CSV do curso tem sujeira de proposito.
    """
    antes = len(df)

    # 1. tira espaco sobrando nos campos de texto
    for col in ["produto", "categoria", "regiao", "canal", "vendedor"]:
        df[col] = df[col].astype(str).str.strip()

    # 2. padroniza a caixa da regiao (tem SUDESTE, sudeste, Sudeste)
    df["regiao"] = df["regiao"].str.title()

    # 3. preco: alguns vem com virgula decimal (padrao BR)
    df["preco_unitario"] = (
        df["preco_unitario"].astype(str).str.replace(",", ".", regex=False)
    )
    df["preco_unitario"] = pd.to_numeric(df["preco_unitario"], errors="coerce")

    # 4. quantidade: alguns vem como texto com espaco
    df["quantidade"] = pd.to_numeric(
        df["quantidade"].astype(str).str.strip(), errors="coerce"
    )

    # 5. data: tem ISO (2025-03-14) e tem BR (14/03/2025)
    #
    #    Cuidado aqui: NAO da pra usar dayfirst=True direto, senao o
    #    pandas inverte dia e mes nas datas que ja estao em ISO.
    #    (2025-01-04 viraria 4 de janeiro... ou 1 de abril. Silenciosamente.)
    #
    #    A solucao: separa quem tem barra de quem nao tem, e converte
    #    cada grupo com o formato certo.
    data = df["data_venda"].astype(str).str.strip()
    tem_barra = data.str.contains("/", na=False)

    df["data_venda"] = pd.NaT
    df.loc[tem_barra, "data_venda"] = pd.to_datetime(
        data[tem_barra], format="%d/%m/%Y", errors="coerce"
    )
    df.loc[~tem_barra, "data_venda"] = pd.to_datetime(
        data[~tem_barra], format="%Y-%m-%d", errors="coerce"
    )

    # 6. campos vazios viram um valor explicito
    #    (o astype(str) la em cima transformou NaN na string "nan",
    #     entao a gente pega os dois casos)
    for col in ["vendedor", "canal"]:
        df[col] = df[col].replace(
            ["", "nan", "NaN", "None", "<NA>"], pd.NA
        )
        df[col] = df[col].fillna("Nao informado")

    # 7. duplicatas
    dups = df.duplicated(subset=["pedido_id"]).sum()
    df = df.drop_duplicates(subset=["pedido_id"], keep="first")

    # 8. o que nao deu pra salvar, sai
    df = df.dropna(subset=["data_venda", "preco_unitario", "quantidade"])

    # 9. a coluna que da valor: o total da linha
    df["valor_total"] = (df["quantidade"] * df["preco_unitario"]).round(2)

    print(f"  {antes} linhas lidas")
    print(f"  {dups} duplicatas removidas")
    print(f"  {antes - dups - len(df)} linhas descartadas (dados invalidos)")
    print(f"  {len(df)} linhas limpas")

    return df


def main():
    print("\n─── ETL de vendas ───\n")

    print("[1/4] Lendo o CSV")
    if not os.path.exists(CSV):
        print(f"  Arquivo nao encontrado: {CSV}")
        print("  Voce copiou a pasta dados/ pra dentro da imagem?")
        sys.exit(1)
    df = pd.read_csv(CSV)
    print(f"  {CSV} — {len(df)} linhas, {len(df.columns)} colunas\n")

    print("[2/4] Limpando")
    df = limpar(df)
    print()

    print("[3/4] Conectando no banco")
    engine = conectar()
    print()

    print("[4/4] Gravando")
    df.to_sql(TABELA, engine, if_exists="replace", index=False)
    print(f"  {len(df)} linhas gravadas na tabela '{TABELA}'\n")

    # prova de que funcionou — e ja e uma analise util
    with engine.connect() as c:
        print("─── Faturamento por regiao ───\n")
        r = c.execute(text(f"""
            SELECT regiao,
                   COUNT(*)                    AS pedidos,
                   ROUND(SUM(valor_total)::numeric, 2)  AS faturamento
            FROM {TABELA}
            GROUP BY regiao
            ORDER BY faturamento DESC
        """))
        print(f"  {'REGIAO':<16} {'PEDIDOS':>8}  {'FATURAMENTO':>16}")
        print(f"  {'-'*16} {'-'*8}  {'-'*16}")
        for regiao, pedidos, fat in r:
            print(f"  {regiao:<16} {pedidos:>8}  {'R$ ' + f'{fat:,.2f}':>16}")

    print("\n─── ETL concluido ───\n")


if __name__ == "__main__":
    main()
