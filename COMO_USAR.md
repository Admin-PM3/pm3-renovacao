# Como Usar — Base de Renovação PM3

Projeto para extrair pagamentos confirmados + certificados e gerar uma base qualificada de alunos para ações de renovação comercial.

---

## 1. Alterar o período

Edite as duas primeiras variáveis no topo de `gerar_base.py`:

```python
DATA_INICIO = "2025-01-01"   # formato: AAAA-MM-DD
DATA_FIM    = "2025-04-30"   # formato: AAAA-MM-DD (inclusive)
```

Salve o arquivo e rode novamente.

---

## 2. Rodar o script principal

```bash
python gerar_base.py
```

Isso gera o arquivo `base_renovacao_AAAAMMDD_AAAAMMDD.xlsx` na mesma pasta.

---

## 3. Rodar o diagnóstico

```bash
python explorar_bancos.py
```

Use para inspecionar os bancos: estrutura das tabelas, status disponíveis, range de datas e amostras de dados.

---

## 4. Status incluídos na análise

| Status       | Significado                                        | Incluído? |
|--------------|----------------------------------------------------|-----------|
| `succeeded`  | Pagamento capturado e confirmado pelo gateway      | ✅ Sim    |
| `authorized` | Pagamento autorizado (renovações/assinaturas)      | ✅ Sim    |
| `error`      | Falha no pagamento                                 | ❌ Não    |
| `pending`    | Aguardando confirmação (ex: PIX não pago)          | ❌ Não    |
| `expired`    | Tentativa expirada                                 | ❌ Não    |
| `failed`     | Recusado pelo banco                                | ❌ Não    |
| outros       | Requerem ação adicional ou estão em processamento  | ❌ Não    |

---

## 5. Estrutura do XLSX gerado

| Aba               | Conteúdo                                                  |
|-------------------|-----------------------------------------------------------|
| Base Qualificada  | Alunos que compraram E têm ao menos 1 certificado emitido |
| Sem Certificado   | Alunos que compraram mas não têm certificado              |
| Resumo            | Métricas gerais + Top 10 cursos + Top 10 produtos         |

---

## 6. Pré-requisitos

- Python instalado (Anaconda: `C:\ProgramData\anaconda3\python.exe`)
- Dependências instaladas via: `pip install -r requirements.txt`
- Arquivo `.env` com as credenciais dos dois bancos (já configurado)

---

## 7. Segurança

- O arquivo `.env` contém credenciais e **nunca deve ser commitado** no Git
- O `.gitignore` já está configurado para ignorar `.env` e arquivos `.xlsx`
- Os scripts são **somente leitura** — nenhum dado é alterado nos bancos

---

*Projeto criado para a equipe PM3 — reutilizável a qualquer momento.*
