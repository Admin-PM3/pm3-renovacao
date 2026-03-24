"""
explorar_bancos.py - Diagnostico dos bancos de dados PM3
Somente leitura. Nunca executa INSERT, UPDATE ou DELETE.
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

# Forcar UTF-8 no stdout para suportar caracteres especiais no Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

load_dotenv()

# ─── Conexões ────────────────────────────────────────────────────────────────

def conectar_pagamentos():
    return psycopg2.connect(
        host=os.getenv("DB_PAYMENTS_HOST"),
        port=int(os.getenv("DB_PAYMENTS_PORT")),
        dbname=os.getenv("DB_PAYMENTS_DB"),
        user=os.getenv("DB_PAYMENTS_USER"),
        password=os.getenv("DB_PAYMENTS_PASSWORD"),
        sslmode="require",
        connect_timeout=15,
    )

def conectar_certificados():
    return psycopg2.connect(
        host=os.getenv("DB_CERTS_HOST"),
        port=int(os.getenv("DB_CERTS_PORT")),
        dbname=os.getenv("DB_CERTS_DB"),
        user=os.getenv("DB_CERTS_USER"),
        password=os.getenv("DB_CERTS_PASSWORD"),
        sslmode="require",
        connect_timeout=15,
    )

def executar(cursor, sql, params=None):
    cursor.execute(sql, params)
    return cursor.fetchall()

# ─── Diagnóstico: Pagamentos ──────────────────────────────────────────────────

def explorar_pagamentos():
    print("\n" + "="*60)
    print("  BANCO 1 — PAGAMENTOS (public.payments)")
    print("="*60)

    conn = conectar_pagamentos()
    cur = conn.cursor()

    # Colunas e tipos
    print("\n📋 COLUNAS E TIPOS:")
    rows = executar(cur, """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'payments'
        ORDER BY ordinal_position
    """)
    for col, dtype in rows:
        print(f"   {col:<35} {dtype}")

    # Status distintos
    print("\n📊 STATUS DISTINTOS (contagem):")
    rows = executar(cur, """
        SELECT status, COUNT(*) as total
        FROM public.payments
        GROUP BY status
        ORDER BY total DESC
    """)
    for status, total in rows:
        print(f"   {str(status):<30} {total:>8} registros")

    # Range de datas
    print("\n📅 RANGE DE DATAS (created_at):")
    rows = executar(cur, """
        SELECT MIN(created_at), MAX(created_at)
        FROM public.payments
    """)
    min_dt, max_dt = rows[0]
    print(f"   Mais antigo: {min_dt}")
    print(f"   Mais recente: {max_dt}")

    # Top 5 produtos
    print("\n🏆 TOP 5 PRODUTOS (product_name):")
    rows = executar(cur, """
        SELECT product_name, COUNT(*) as total
        FROM public.payments
        GROUP BY product_name
        ORDER BY total DESC
        LIMIT 5
    """)
    for i, (prod, total) in enumerate(rows, 1):
        print(f"   {i}. {str(prod):<50} {total:>6}")

    # Amostra de 3 registros
    print("\n🔍 AMOSTRA — 3 REGISTROS COMPLETOS:")
    rows = executar(cur, "SELECT * FROM public.payments LIMIT 3")
    col_names = [desc[0] for desc in cur.description]
    for i, row in enumerate(rows, 1):
        print(f"\n  Registro {i}:")
        for col, val in zip(col_names, row):
            print(f"    {col:<35} {val}")

    cur.close()
    conn.close()
    print("\n✅ Conexão com Pagamentos encerrada.")

# ─── Diagnóstico: Certificados ────────────────────────────────────────────────

def explorar_certificados():
    print("\n" + "="*60)
    print("  BANCO 2 — CERTIFICADOS (prod.cursos_certificados)")
    print("="*60)

    conn = conectar_certificados()
    cur = conn.cursor()

    # Colunas e tipos
    print("\n📋 COLUNAS E TIPOS:")
    rows = executar(cur, """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'prod' AND table_name = 'cursos_certificados'
        ORDER BY ordinal_position
    """)
    for col, dtype in rows:
        print(f"   {col:<35} {dtype}")

    # Total de registros
    print("\n📊 TOTAL DE REGISTROS:")
    rows = executar(cur, "SELECT COUNT(*) FROM prod.cursos_certificados")
    print(f"   {rows[0][0]:,} registros")

    # Top 5 cursos
    print("\n🏆 TOP 5 CURSOS MAIS FREQUENTES:")
    rows = executar(cur, """
        SELECT curso, COUNT(*) as total
        FROM prod.cursos_certificados
        GROUP BY curso
        ORDER BY total DESC
        LIMIT 5
    """)
    for i, (curso, total) in enumerate(rows, 1):
        print(f"   {i}. {str(curso):<50} {total:>6}")

    # Amostra de 3 registros
    print("\n🔍 AMOSTRA — 3 REGISTROS COMPLETOS:")
    rows = executar(cur, "SELECT * FROM prod.cursos_certificados LIMIT 3")
    col_names = [desc[0] for desc in cur.description]
    for i, row in enumerate(rows, 1):
        print(f"\n  Registro {i}:")
        for col, val in zip(col_names, row):
            print(f"    {col:<35} {val}")

    cur.close()
    conn.close()
    print("\n✅ Conexão com Certificados encerrada.")

# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n🔎 DIAGNÓSTICO DOS BANCOS DE DADOS PM3")
    print("   (somente leitura — nenhum dado será alterado)\n")

    try:
        explorar_pagamentos()
    except Exception as e:
        print(f"\n❌ Erro ao conectar em Pagamentos: {e}")

    try:
        explorar_certificados()
    except Exception as e:
        print(f"\n❌ Erro ao conectar em Certificados: {e}")

    print("\n" + "="*60)
    print("  FIM DO DIAGNÓSTICO")
    print("="*60 + "\n")
