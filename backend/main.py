from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os
from datetime import datetime, timedelta
from lxml import etree

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://app:app@localhost:5432/estoque")
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

app = FastAPI(title="Estoque Supermercado", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- SQL (DDL) simples: cria tabelas se não existirem ----
with engine.begin() as conn:
    conn.exec_driver_sql(
        """
        CREATE TABLE IF NOT EXISTS produtos (
            id SERIAL PRIMARY KEY,
            codigo VARCHAR(64) UNIQUE,
            ean VARCHAR(32),
            descricao TEXT NOT NULL,
            ncm VARCHAR(16),
            unidade VARCHAR(8),
            preco_medio NUMERIC(14,4) DEFAULT 0,
            estoque_atual NUMERIC(14,4) DEFAULT 0,
            consumo_medio_dia NUMERIC(14,4) DEFAULT 0,
            lead_time_dias INT DEFAULT 7,
            fator_seguranca NUMERIC(6,2) DEFAULT 1.2,
            criado_em TIMESTAMP DEFAULT NOW(),
            atualizado_em TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS movimentos (
            id SERIAL PRIMARY KEY,
            produto_id INT REFERENCES produtos(id) ON DELETE CASCADE,
            tipo VARCHAR(16) NOT NULL, -- ENTRADA | SAIDA | AJUSTE
            quantidade NUMERIC(14,4) NOT NULL,
            preco_unit NUMERIC(14,4) DEFAULT 0, -- usado p/ média
            origem VARCHAR(32), -- XML | EXCEL | MANUAL
            documento VARCHAR(128),
            data_mov TIMESTAMP DEFAULT NOW()
        );
        """
    )

# ---- Schemas ----
class ProdutoIn(BaseModel):
    codigo: Optional[str] = None
    ean: Optional[str] = None
    descricao: str
    ncm: Optional[str] = None
    unidade: Optional[str] = "UN"
    lead_time_dias: Optional[int] = 7
    fator_seguranca: Optional[float] = 1.2

class ProdutoOut(ProdutoIn):
    id: int
    preco_medio: float
    estoque_atual: float

class MovimentoIn(BaseModel):
    produto_id: int
    tipo: str
    quantidade: float
    preco_unit: Optional[float] = 0
    origem: Optional[str] = "MANUAL"
    documento: Optional[str] = None
    data_mov: Optional[datetime] = None

# ---- Regras de negócio ----

def atualizar_preco_medio_e_estoque(conn, produto_id: int, tipo: str, quantidade: float, preco_unit: float):
    row = conn.execute(text("SELECT estoque_atual, preco_medio FROM produtos WHERE id=:id"), {"id": produto_id}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Produto não encontrado")

    estoque_atual = float(row["estoque_atual"]) if row["estoque_atual"] is not None else 0.0
    preco_medio = float(row["preco_medio"]) if row["preco_medio"] is not None else 0.0

    if tipo.upper() == "ENTRADA":
        total_valor = estoque_atual * preco_medio + quantidade * preco_unit
        novo_estoque = estoque_atual + quantidade
        novo_preco = (total_valor / novo_estoque) if novo_estoque > 0 else preco_medio
    elif tipo.upper() == "SAIDA":
        novo_estoque = max(0.0, estoque_atual - quantidade)
        novo_preco = preco_medio
    else:  # AJUSTE
        novo_estoque = max(0.0, estoque_atual + quantidade)
        novo_preco = preco_medio

    conn.execute(
        text("UPDATE produtos SET estoque_atual=:e, preco_medio=:p, atualizado_em=NOW() WHERE id=:id"),
        {"e": novo_estoque, "p": novo_preco, "id": produto_id}
    )

def calcular_consumo_medio(conn, produto_id: int, dias: int = 30):
    since = datetime.now() - timedelta(days=dias)
    q = conn.execute(
        text("""
            SELECT COALESCE(SUM(quantidade),0) as q
            FROM movimentos WHERE produto_id=:id AND tipo='SAIDA' AND data_mov>=:since
        """), {"id": produto_id, "since": since}
    ).scalar_one()
    consumo_medio = float(q) / dias if dias > 0 else 0.0
    conn.execute(text("UPDATE produtos SET consumo_medio_dia=:c WHERE id=:id"), {"c": consumo_medio, "id": produto_id})
    return consumo_medio

def calcular_min_max_sugestao(conn, produto_id: int):
    row = conn.execute(text("SELECT estoque_atual, consumo_medio_dia, lead_time_dias, fator_seguranca FROM produtos WHERE id=:id"), {"id": produto_id}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Produto não encontrado")

    consumo = float(row["consumo_medio_dia"]) or 0.0
    lead = int(row["lead_time_dias"]) or 7
    fs = float(row["fator_seguranca"]) or 1.2

    estoque_min = consumo * lead
    estoque_max = estoque_min * fs
    sugerido = max(0.0, estoque_max - float(row["estoque_atual"]))
    return {
        "estoque_min": round(estoque_min, 2),
        "estoque_max": round(estoque_max, 2),
        "sugestao_compra": round(sugerido, 2),
    }

def calcular_giro(conn, dias: int = 30):
    produtos = conn.execute(text("SELECT id, preco_medio FROM produtos")).mappings().all()
    since = datetime.now() - timedelta(days=dias)
    cogs = 0.0
    estoque_medio = 0.0
    for p in produtos:
        saida_q = conn.execute(text("SELECT COALESCE(SUM(quantidade),0) FROM movimentos WHERE produto_id=:id AND tipo='SAIDA' AND data_mov>=:since"), {"id": p["id"], "since": since}).scalar_one()
        cogs += float(saida_q) * float(p["preco_medio"] or 0)
        estoque_medio += float(conn.execute(text("SELECT COALESCE(AVG(estoque_atual),0) FROM (SELECT estoque_atual FROM movimentos m JOIN produtos pr ON pr.id=m.produto_id WHERE produto_id=:id AND data_mov>=:since ORDER BY data_mov) t"), {"id": p["id"], "since": since}).scalar_one() or 0)
    giro = (cogs / estoque_medio) if estoque_medio > 0 else 0.0
    return {"periodo_dias": dias, "cogs_aprox": round(cogs,2), "estoque_medio_aprox": round(estoque_medio,2), "giro": round(giro,2)}

def curva_abc(conn, dias: int = 90):
    since = datetime.now() - timedelta(days=dias)
    rows = conn.execute(text("""
        SELECT p.id, p.descricao, p.preco_medio, COALESCE(SUM(CASE WHEN m.tipo='SAIDA' THEN m.quantidade ELSE 0 END),0) as q_saida
        FROM produtos p
        LEFT JOIN movimentos m ON m.produto_id=p.id AND m.data_mov>=:since
        GROUP BY p.id
    """), {"since": since}).mappings().all()
    itens = []
    total_valor = 0.0
    for r in rows:
        valor = float(r["q_saida"]) * float(r["preco_medio"] or 0)
        total_valor += valor
        itens.append({"id": r["id"], "descricao": r["descricao"], "valor": valor})
    itens.sort(key=lambda x: x["valor"], reverse=True)
    acumulado = 0.0
    out = []
    for it in itens:
        perc = (it["valor"] / total_valor * 100) if total_valor > 0 else 0
        acumulado += perc
        classe = 'A' if acumulado <= 80 else ('B' if acumulado <= 95 else 'C')
        out.append({**it, "perc": round(perc,2), "acumulado": round(acumulado,2), "classe": classe})
    return out

# ---- Endpoints ----

@app.get("/produtos", response_model=List[ProdutoOut])
def listar_produtos():
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT * FROM produtos ORDER BY descricao")).mappings().all()
        return [ProdutoOut(id=r["id"], codigo=r["codigo"], ean=r["ean"], descricao=r["descricao"], ncm=r["ncm"], unidade=r["unidade"], lead_time_dias=r["lead_time_dias"], fator_seguranca=float(r["fator_seguranca"] or 1.2), preco_medio=float(r["preco_medio"] or 0), estoque_atual=float(r["estoque_atual"] or 0)) for r in rows]

@app.post("/produtos", response_model=ProdutoOut)
def criar_produto(p: ProdutoIn):
    with engine.begin() as conn:
        res = conn.execute(text("""
            INSERT INTO produtos(codigo, ean, descricao, ncm, unidade, lead_time_dias, fator_seguranca)
            VALUES (:codigo,:ean,:descricao,:ncm,:unidade,:lead,:fs)
            RETURNING *
        """), {"codigo": p.codigo, "ean": p.ean, "descricao": p.descricao, "ncm": p.ncm, "unidade": p.unidade, "lead": p.lead_time_dias, "fs": p.fator_seguranca}).mappings().first()
        return ProdutoOut(id=res["id"], codigo=res["codigo"], ean=res["ean"], descricao=res["descricao"], ncm=res["ncm"], unidade=res["unidade"], lead_time_dias=res["lead_time_dias"], fator_seguranca=float(res["fator_seguranca"] or 1.2), preco_medio=float(res["preco_medio"] or 0), estoque_atual=float(res["estoque_atual"] or 0))

@app.post("/movimentos")
def lancar_movimento(m: MovimentoIn):
    if m.tipo.upper() not in {"ENTRADA","SAIDA","AJUSTE"}:
        raise HTTPException(status_code=400, detail="Tipo inválido")
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO movimentos(produto_id, tipo, quantidade, preco_unit, origem, documento, data_mov)
            VALUES (:pid,:tipo,:q,:preco,:origem,:doc,:data)
        """), {"pid": m.produto_id, "tipo": m.tipo.upper(), "q": m.quantidade, "preco": m.preco_unit or 0, "origem": m.origem, "doc": m.documento, "data": m.data_mov or datetime.now()})
        atualizar_preco_medio_e_estoque(conn, m.produto_id, m.tipo, m.quantidade, m.preco_unit or 0)
        calcular_consumo_medio(conn, m.produto_id)
    return {"ok": True}

@app.post("/import/xml-nfe")
async def importar_xml_nfe(file: UploadFile = File(...)):
    content = await file.read()
    try:
        root = etree.fromstring(content)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"XML inválido: {e}")

    ns = {"nfe": "http://www.portalfiscal.inf.br/nfe"}
    itens = root.xpath(".//nfe:det", namespaces=ns)
    if not itens:
        itens = root.xpath(".//det")
    inseridos = 0

    with engine.begin() as conn:
        for det in itens:
            prod = det.xpath(".//nfe:prod", namespaces=ns)
            prod = prod[0] if prod else det.find("prod")
            if prod is None:
                continue
            def tx(tag):
                return (prod.findtext(f"{{http://www.portalfiscal.inf.br/nfe}}{tag}") or prod.findtext(tag))

            codigo = tx("cProd")
            ean = tx("cEAN")
            desc = tx("xProd") or "SEM DESCRICAO"
            ncm = tx("NCM")
            unidade = tx("uCom") or "UN"
            q = float((tx("qCom") or 0))
            v = float((tx("vUnCom") or 0))

            r = conn.execute(text("SELECT id FROM produtos WHERE codigo=:c"), {"c": codigo}).mappings().first()
            if r:
                pid = r["id"]
                conn.execute(text("UPDATE produtos SET ean=:ean, descricao=:d, ncm=:n, unidade=:u, atualizado_em=NOW() WHERE id=:id"), {"ean": ean, "d": desc, "n": ncm, "u": unidade, "id": pid})
            else:
                pid = conn.execute(text("""
                    INSERT INTO produtos(codigo, ean, descricao, ncm, unidade)
                    VALUES (:c,:ean,:d,:n,:u) RETURNING id
                """), {"c": codigo, "ean": ean, "d": desc, "n": ncm, "u": unidade}).scalar_one()

            conn.execute(text("""
                INSERT INTO movimentos(produto_id, tipo, quantidade, preco_unit, origem, documento)
                VALUES (:pid,'ENTRADA',:q,:v,'XML',:doc)
            """), {"pid": pid, "q": q, "v": v, "doc": file.filename})
            atualizar_preco_medio_e_estoque(conn, pid, "ENTRADA", q, v)
            calcular_consumo_medio(conn, pid)
            inseridos += 1

    return {"itens_processados": inseridos}

@app.post("/import/excel")
async def importar_excel(file: UploadFile = File(...)):
    content = await file.read()
    df = None
    try:
        df = pd.read_excel(content)
    except Exception:
        try:
            df = pd.read_csv(content)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Arquivo inválido: {e}")

    required = {"codigo","descricao","tipo","quantidade"}
    cols_lower = [c.lower() for c in df.columns]
    if not required.issubset(set(cols_lower)):
        raise HTTPException(status_code=400, detail=f"Planilha deve conter: {required}")

    df.columns = cols_lower
    inseridos = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            codigo = (str(row.get("codigo") or "").strip() or None)
            desc = str(row.get("descricao") or "SEM DESCRICAO")
            ean = str(row.get("ean") or None)
            ncm = str(row.get("ncm") or None)
            unidade = str(row.get("unidade") or "UN")
            tipo = str(row.get("tipo") or "ENTRADA").upper()
            quantidade = float(row.get("quantidade") or 0)
            preco_unit = float(row.get("preco_unit") or 0)

            if tipo not in {"ENTRADA","SAIDA","AJUSTE"}:
                continue

            pid = None
            if codigo:
                r = conn.execute(text("SELECT id FROM produtos WHERE codigo=:c"), {"c": codigo}).mappings().first()
                if r:
                    pid = r["id"]
                    conn.execute(text("UPDATE produtos SET ean=:ean, descricao=:d, ncm=:n, unidade=:u WHERE id=:id"), {"ean": ean, "d": desc, "n": ncm, "u": unidade, "id": pid})
            if not pid:
                pid = conn.execute(text("INSERT INTO produtos(codigo, ean, descricao, ncm, unidade) VALUES (:c,:ean,:d,:n,:u) RETURNING id"), {"c": codigo, "ean": ean, "d": desc, "n": ncm, "u": unidade}).scalar_one()

            conn.execute(text("INSERT INTO movimentos(produto_id,tipo,quantidade,preco_unit,origem,documento) VALUES (:pid,:t,:q,:p,'EXCEL',:doc)"), {"pid": pid, "t": tipo, "q": quantidade, "p": preco_unit, "doc": file.filename})
            atualizar_preco_medio_e_estoque(conn, pid, tipo, quantidade, preco_unit)
            calcular_consumo_medio(conn, pid)
            inseridos += 1

    return {"linhas_processadas": inseridos}

@app.get("/sugestoes-compra/{produto_id}")
def sugestao(produto_id: int):
    with engine.begin() as conn:
        return calcular_min_max_sugestao(conn, produto_id)

@app.get("/giro")
def giro(dias: int = 30):
    with engine.begin() as conn:
        return calcular_giro(conn, dias)

@app.get("/abc")
def abc(dias: int = 90):
    with engine.begin() as conn:
        return curva_abc(conn, dias)

@app.get("/capital-de-giro")
def capital_de_giro(dias: int = 30):
    with engine.begin() as conn:
        valor_estoque = conn.execute(text("SELECT COALESCE(SUM(estoque_atual*preco_medio),0) FROM produtos")).scalar_one()
    return {"dias": dias, "valor_estoque": float(valor_estoque), "contas_pagar": 0.0, "contas_receber": 0.0, "capital_giro_aprox": float(valor_estoque)}
