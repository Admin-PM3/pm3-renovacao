"""
api/index.py - Vercel Serverless entrypoint

Para WSGI (Flask), a Vercel espera apenas a variável 'app'.
NÃO usar 'handler' — o runtime interpreta como BaseHTTPRequestHandler e quebra.
"""
import sys
import os

# Adiciona o diretório raiz ao path (app.py, gerar_base.py, templates/)
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

from app import app  # 'app' é a variável que a Vercel detecta como WSGI
