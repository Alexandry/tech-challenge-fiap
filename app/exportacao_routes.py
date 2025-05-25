from fastapi import APIRouter, status, HTTPException, Query
from typing import List
import requests
from app.model.dados_comerciais import DadosComerciais, DadosComerciaisPage
from app.services import embrapa_service, cache_service
from app.mongo import db
from pymongo import UpdateOne
from pymongo.errors import PyMongoError

router = APIRouter()

@router.get('/exportacao', response_model=DadosComerciaisPage, summary="Dados de exportação da Embrapa")
async def get_exportacao(page: int = Query(1, ge=1), size: int = Query(10, ge=1)):

    cache_key = f"exportacao:{page}:{size}"
    cached = cache_service.get_cache(cache_key)
    if cached:
        return DadosComerciaisPage(**cached)
    
    url = 'http://vitibrasil.cnpuv.embrapa.br/index.php?opcao=opt_06'
    response = requests.get(url)
    response.encoding = 'utf-8'
    dados_extraidos = embrapa_service.extrair_exportacao_importacao(response.text)
    registros = [DadosComerciais(**d) for d in dados_extraidos]

    

    operations = [
        UpdateOne(
            {"pais": r.pais, "quantidade_kg": r.quantidade_kg, "valor_usd": r.valor_usd},
            {"$setOnInsert": r.dict()},
            upsert=True
        ) for r in registros
    ]

    start = (page - 1) * size
    end = start + size
    paginados = registros[start:end]

    try:
        db.exportacao.bulk_write(operations, ordered=False)
    except PyMongoError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    
    total_pages = (len(registros) + size - 1) // size

    result = DadosComerciaisPage(
        items=paginados,
        total=len(registros),
        skip=start,
        limit=size,
        total_pages=total_pages,
        page=page
    )
    cache_service.set_cache(cache_key, result.dict())
    return result
