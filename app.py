"""
app.py - Interface web do Criador de base PM3
Chama a logica ja existente em gerar_base.py.
Pronto para deploy no Railway (host 0.0.0.0, PORT via env var).
"""

import os
import sys
import logging
import traceback
from flask import Flask, render_template, request, jsonify, send_file

# Forcar UTF-8
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

# Logging estruturado para Railway
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

app = Flask(__name__)


def _validar_datas(data_inicio, data_fim):
    from datetime import datetime
    try:
        d1 = datetime.strptime(data_inicio, '%Y-%m-%d')
        d2 = datetime.strptime(data_fim, '%Y-%m-%d')
        if d1 > d2:
            return "Data inicial deve ser anterior à data final."
    except (ValueError, TypeError):
        return "Datas inválidas. Use o formato AAAA-MM-DD."
    return None


@app.route('/health')
def health():
    """Health check para Railway / monitoramento."""
    return jsonify({'status': 'ok'}), 200


@app.route('/')
def index():
    logger.info("Pagina inicial acessada")
    return render_template('index.html')


@app.route('/buscar', methods=['POST'])
def buscar():
    body = request.get_json(force=True, silent=True) or {}
    data_inicio = body.get('data_inicio', '').strip()
    data_fim    = body.get('data_fim', '').strip()

    erro = _validar_datas(data_inicio, data_fim)
    if erro:
        return jsonify({'error': erro}), 400

    try:
        logger.info(f"Busca iniciada: {data_inicio} a {data_fim}")
        from gerar_base import run_pipeline
        df_qual, df_sem, df_recente, prox30, prox90 = run_pipeline(data_inicio, data_fim)

        total_qualificados = len(df_qual)
        total_sem_cert     = len(df_sem)
        total_clientes     = total_qualificados + total_sem_cert

        if total_clientes == 0:
            return jsonify({'registros': [], 'total_clientes': 0,
                            'total_qualificados': 0, 'total_sem_cert': 0,
                            'prox30': 0, 'prox90': 0, 'produtos': []})

        # Serializar para JSON (NaN → "")
        registros = (
            df_qual
            .fillna('')
            .astype(str)
            .replace({'nan': '', '<NA>': ''})
            .to_dict(orient='records')
        )
        # Converter "Quantos certificados emitidos" de volta para int quando possível
        for r in registros:
            try:
                v = r.get('Quantos certificados emitidos', '')
                r['Quantos certificados emitidos'] = int(float(v)) if v not in ('', 'nan') else ''
            except Exception:
                pass

        produtos = sorted(
            p for p in df_qual['Produto que ja comprou'].dropna().unique().tolist()
            if p
        )

        tipos = sorted(
            t for t in df_qual['Tipo de cliente'].dropna().unique().tolist()
            if t
        )

        logger.info(f"Busca concluida: {total_qualificados} qualificados, {total_sem_cert} sem cert")
        return jsonify({
            'registros':          registros,
            'total_clientes':     total_clientes,
            'total_qualificados': total_qualificados,
            'total_sem_cert':     total_sem_cert,
            'prox30':             prox30,
            'prox90':             prox90,
            'produtos':           produtos,
            'tipos':              tipos,
        })

    except Exception as e:
        logger.error(f"Erro na busca: {e}\n{traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@app.route('/exportar', methods=['POST'])
def exportar():
    body = request.get_json(force=True, silent=True) or {}
    data_inicio = body.get('data_inicio', '').strip()
    data_fim    = body.get('data_fim', '').strip()

    erro = _validar_datas(data_inicio, data_fim)
    if erro:
        return jsonify({'error': erro}), 400

    try:
        logger.info(f"Exportacao XLSX iniciada: {data_inicio} a {data_fim}")
        from gerar_base import run_pipeline, build_xlsx_bytes
        df_qual, df_sem, df_recente, prox30, prox90 = run_pipeline(data_inicio, data_fim)
        xlsx = build_xlsx_bytes(df_qual, df_sem, df_recente, prox30, prox90, data_inicio, data_fim)

        d1 = data_inicio.replace('-', '')
        d2 = data_fim.replace('-', '')
        filename = f"base_renovacao_{d1}_{d2}.xlsx"

        logger.info(f"Exportacao XLSX concluida: {filename}")
        return send_file(
            xlsx,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename,
        )

    except Exception as e:
        logger.error(f"Erro na exportacao: {e}\n{traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    logger.info(f"Servidor iniciando em 0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port, debug=False)
