# Estoque de Supermercado (Web, 100% Online)

## Como publicar no Render (sem instalar nada no seu computador)
1. Crie um repositório no GitHub e envie estes arquivos.
2. No Render, clique **New + → Blueprint** e selecione o repositório.
3. Confirme os serviços (backend, frontend e banco).
4. Aguarde o backend publicar e **copie a URL pública** dele.
5. Edite `frontend/config.js` e troque `https://RENDER-BACKEND-URL` pela URL pública do backend (ex.: `https://estoque-backend-xxxxx.onrender.com`).
6. Faça um novo deploy do **frontend** (ou ative auto-deploy).
7. Acesse a URL pública do **frontend** pelo navegador.

## Importações
- **XML de NF-e**: botão "Importar XML (NF-e)".
- **Excel/CSV**: cabeçalhos esperados (minúsculos): `codigo,ean,descricao,ncm,unidade,tipo,quantidade,preco_unit`.

## Desenvolvimento local (opcional)
```bash
docker compose up -d --build
# app: http://localhost:8080   api: http://localhost:8000/docs
```
