from pydantic import BaseModel
from typing import List

class DadosComerciais(BaseModel):
    pais: str
    quantidade_kg: str
    valor_usd: str

class DadosComerciaisPage(BaseModel):
    items: List[DadosComerciais]
    total: int
    skip: int
    limit: int
    total_pages: int
    page: int
    
    class Config:
        json_schema_extra = {
            "example": {
               "items": [
                    {
                        "pais": "Brasil",
                        "quantidade_kg": "1.000",
                        "valor_usd": "$5.000,00"
                    }
                ],
                "total": 100,
                "skip": 0,
                "limit": 10,
                "total_pages": 10,
                "page": 1
            }
        }