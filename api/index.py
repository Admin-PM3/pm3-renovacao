"""
api/index.py - Vercel Serverless entrypoint
Importa o Flask app do diretório raiz.
A Vercel detecta automaticamente apps WSGI/Flask — sem app.run(), sem host, sem porta.
"""
import sys
import os

# Adiciona o diretório raiz ao path para que app.py e gerar_base.py sejam encontrados
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app  # noqa: F401  (Vercel detecta via nome 'app')

# 'handler' é o alias para compatibilidade com runtimes que procuram essa variável
handler = app
