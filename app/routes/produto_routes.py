import logging
from fastapi import APIRouter, status, HTTPException, Query
from typing import List, Tuple, Optional
import requests
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError, PyMongoError
from app.model.produto import Produto, ProdutoPage
from app.services import embrapa_service, cache_service
from app.mongo import db

router = APIRouter()
logger = logging.getLogger("uvicorn.error")


def _build_operations(produtos: List[Produto]) -> List[UpdateOne]:
    """
    Deduplica produtos por (categoria, tipo_produto) e cria operações upsert.
    """
    seen: set[Tuple[str, str]] = set()
    ops: List[UpdateOne] = []
    for p in produtos:
        key = (p.categoria, p.tipo_produto)
        if key in seen:
            continue
        seen.add(key)
        ops.append(
            UpdateOne(
                {"categoria": p.categoria, "tipo_produto": p.tipo_produto},
                {"$setOnInsert": p.dict(exclude_none=True)},
                upsert=True
            )
        )
    return ops


@router.get(
    "/producao", response_model=ProdutoPage, summary="Dados de produção da Embrapa"
)
async def get_producao(page: int = Query(1, ge=1),
                       size: int = Query(10, ge=1),
                       categoria: Optional[str] = Query(None, description="Filtrar por categoria"),
                       tipo_produto: Optional[str] = Query(None, description="Filtrar por tipo de produto")):
    
    cache_key = f"producao:{page}:{size}:{categoria}:{tipo_produto}"
    cached = cache_service.get_cache(cache_key)
    if cached:
        return ProdutoPage(**cached)
    
    try:
        resp = requests.get(
            "http://vitibrasil.cnpuv.embrapa.br/index.php?opcao=opt_02",
            timeout=10
        )

        resp.encoding = "utf-8"
        dados = embrapa_service.extrair_dados_tabela(resp.text)
        produtos = [Produto(**d) for d in dados]
        ops = _build_operations(produtos)
        print(categoria)
        if categoria:
            produtos = [p for p in produtos if categoria.lower() in p.categoria.lower()]
            print("Filtrando por categoria:", categoria)
        if tipo_produto:
            produtos = [p for p in produtos if tipo_produto.lower() in p.tipo_produto.lower()]
  
        start = (page - 1) * size
        end = start + size
        paginados = produtos[start:end]

        if ops:
            try:
                db.producao.bulk_write(ops, ordered=False)
            except BulkWriteError as bwe:
                logger.warning(f"BulkWriteWarning produção: {bwe.details}")

        total_pages = (len(produtos) + size - 1) // size

        result = ProdutoPage(
            items=paginados,
            total=len(produtos),
            skip=start,
            limit=size,
            total_pages=total_pages,
            page=page
        )
        cache_service.set_cache(cache_key, result.dict())
        return result
    except requests.RequestException as re:
        logger.error(f"Erro de request produção: {re}")
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail="Falha ao conectar Embrapa")
    except PyMongoError as me:
        logger.error(f"Erro Mongo produção: {me}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao salvar dados produção")
    except Exception as e:
        logger.exception("Erro inesperado em produção")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get(
    "/processamento", response_model=ProdutoPage, summary="Dados de processamento da Embrapa"
)
async def get_processamento(page: int = Query(1, ge=1),
                            size: int = Query(10, ge=1),
                            categoria: Optional[str] = Query(None, description="Filtrar por categoria"),
                            tipo_produto: Optional[str] = Query(None, description="Filtrar por tipo de produto")):

    cache_key = f"processamento:{page}:{size}:{categoria}:{tipo_produto}"
    cached = cache_service.get_cache(cache_key)
    if cached:
        return ProdutoPage(**cached)
    
    try:
        resp = requests.get(
            "http://vitibrasil.cnpuv.embrapa.br/index.php?opcao=opt_03",
            timeout=10
        )
        resp.encoding = "utf-8"
        dados = embrapa_service.extrair_dados_tabela(resp.text)
        produtos = [Produto(**d) for d in dados]

        if categoria:
            produtos = [p for p in produtos if categoria.lower() in p.categoria.lower()]
            print("Filtrando por categoria:", categoria)
        if tipo_produto:
            produtos = [p for p in produtos if tipo_produto.lower() in p.tipo_produto.lower()]
        
        start = (page - 1) * size
        end = start + size
        paginados = produtos[start:end]

        ops = _build_operations(produtos)
        if ops:
            try:
                db.processamento.bulk_write(ops, ordered=False)
            except BulkWriteError as bwe:
                logger.warning(f"BulkWriteWarning processamento: {bwe.details}")

        total_pages = (len(produtos) + size - 1) // size

        result = ProdutoPage(
            items=paginados,
            total=len(produtos),
            skip=start,
            limit=size,
            total_pages=total_pages,
            page=page
        )
        cache_service.set_cache(cache_key, result.dict())
        return result
    except requests.RequestException as re:
        logger.error(f"Erro de request processamento: {re}")
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail="Falha ao conectar Embrapa")
    except PyMongoError as me:
        logger.error(f"Erro Mongo processamento: {me}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao salvar dados processamento")
    except Exception as e:
        logger.exception("Erro inesperado em processamento")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get(
    "/comercializacao", response_model=ProdutoPage, summary="Dados de comercialização da Embrapa"
)
async def get_comercializacao(page: int = Query(1, ge=1),
                              size: int = Query(10, ge=1),
                              categoria: Optional[str] = Query(None, description="Filtrar por categoria"),
                              tipo_produto: Optional[str] = Query(None, description="Filtrar por tipo de produto")):
    
    cache_key = f"comercializacao:{page}:{size}:{categoria}:{tipo_produto}"
    cached = cache_service.get_cache(cache_key)
    if cached:
        return ProdutoPage(**cached)

    try:
        resp = requests.get(
            "http://vitibrasil.cnpuv.embrapa.br/index.php?opcao=opt_04",
            timeout=10
        )
        resp.encoding = "utf-8"
        dados = embrapa_service.extrair_dados_tabela(resp.text)
        produtos = [Produto(**d) for d in dados]

        if categoria:
            produtos = [p for p in produtos if categoria.lower() in p.categoria.lower()]
            print("Filtrando por categoria:", categoria)
        if tipo_produto:
            produtos = [p for p in produtos if tipo_produto.lower() in p.tipo_produto.lower()]
        
        start = (page - 1) * size
        end = start + size
        paginados = produtos[start:end]

        ops = _build_operations(produtos)       

        if ops:
            try:
                db.comercializacao.bulk_write(ops, ordered=False)
            except BulkWriteError as bwe:
                logger.warning(f"BulkWriteWarning comercialização: {bwe.details}")

        total_pages = (len(produtos) + size - 1) // size

        result = ProdutoPage(
            items=paginados,
            total=len(produtos),
            skip=start,
            limit=size,
            total_pages=total_pages,
            page=page
        )
        cache_service.set_cache(cache_key, result.dict())
        return result
    except requests.RequestException as re:
        logger.error(f"Erro de request comercialização: {re}")
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail="Falha ao conectar Embrapa")
    except PyMongoError as me:
        logger.error(f"Erro Mongo comercialização: {me}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao salvar dados comercialização")
    except Exception as e:
        logger.exception("Erro inesperado em comercialização")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))