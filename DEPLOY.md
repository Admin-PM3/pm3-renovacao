# Deploy no Railway — Criador de base PM3

## 1. Preparar o repositorio no GitHub

```bash
cd pm3-renovacao
git init
git add .
git commit -m "Projeto Criador de base PM3"
```

Crie um repositorio no GitHub (ex: `pm3-renovacao`) e envie:

```bash
git remote add origin https://github.com/SEU_USUARIO/pm3-renovacao.git
git branch -M main
git push -u origin main
```

> **IMPORTANTE:** O arquivo `.env` esta no `.gitignore` e NAO sera enviado ao GitHub.
> As credenciais serao configuradas diretamente no Railway (passo 3).

---

## 2. Criar o projeto no Railway

1. Acesse [railway.com](https://railway.com) e faca login (pode usar conta GitHub)
2. Clique em **"New Project"**
3. Selecione **"Deploy from GitHub Repo"**
4. Autorize o acesso e escolha o repositorio `pm3-renovacao`
5. O Railway detecta automaticamente o `Procfile` e `railway.json`
6. O deploy inicia automaticamente

---

## 3. Adicionar variaveis de ambiente no Railway

No painel do projeto Railway:

1. Clique no servico (o card do deploy)
2. Va na aba **"Variables"**
3. Adicione cada variavel abaixo (clique em "New Variable" para cada uma):

| Variavel             | Valor                                                                 |
|----------------------|-----------------------------------------------------------------------|
| DB_PAYMENTS_HOST     | c4d0u95821050m.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com     |
| DB_PAYMENTS_PORT     | 5432                                                                  |
| DB_PAYMENTS_DB       | de3f091k55cn2r                                                        |
| DB_PAYMENTS_USER     | ue65qrndoj4tqa                                                        |
| DB_PAYMENTS_PASSWORD | (copie da .env local)                                                 |
| DB_CERTS_HOST        | pm3-db-do-user-17355541-0.d.db.ondigitalocean.com                    |
| DB_CERTS_PORT        | 25060                                                                 |
| DB_CERTS_DB          | kajabi                                                                |
| DB_CERTS_USER        | pm3_normal_user                                                       |
| DB_CERTS_PASSWORD    | (copie da .env local)                                                 |

> A variavel `PORT` e configurada automaticamente pelo Railway. Nao precisa adicionar.

4. Apos adicionar, o Railway faz redeploy automatico

---

## 4. Acessar a URL publica

1. No painel do projeto, clique em **"Settings"** do servico
2. Na secao **"Networking"** > **"Public Networking"**
3. Clique em **"Generate Domain"**
4. Sera gerada uma URL como: `https://pm3-renovacao-production-xxxx.up.railway.app`
5. Compartilhe essa URL com a equipe — qualquer pessoa com o link acessa pelo navegador

---

## 5. Verificar se esta funcionando

- Acesse `https://SUA_URL.up.railway.app/health` — deve retornar `{"status": "ok"}`
- Acesse a URL raiz para ver a interface
- Consulte os logs em tempo real na aba **"Deployments"** > **"View Logs"**

---

## 6. Atualizar o projeto

Qualquer `git push` para a branch `main` dispara um redeploy automatico:

```bash
git add .
git commit -m "Atualizacao"
git push
```

O Railway detecta a mudanca e faz deploy da nova versao em segundos.

---

## Troubleshooting

| Problema                         | Solucao                                                    |
|----------------------------------|------------------------------------------------------------|
| App nao inicia                   | Verifique as variaveis de ambiente na aba Variables        |
| Erro de conexao com banco        | Verifique se host/port/password estao corretos             |
| Timeout nas consultas            | O timeout do gunicorn e 120s — consultas grandes demoram   |
| Pagina em branco                 | Veja os logs em Deployments > View Logs                    |
| "Application failed to respond"  | O PORT deve ser lido de os.environ (ja esta configurado)   |
