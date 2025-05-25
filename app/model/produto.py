from pydantic import BaseModel, Field
from typing import List

class Produto(BaseModel):
    categoria: str
    tipo_produto: str
    quantidade: str

class ProdutoPage(BaseModel):
    items: List[Produto]
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
                        "categoria": "Vinho Tinto",
                        "tipo_produto": "Tinto",
                        "quantidade": "139.320.884"
                    }
                ],
                "total": 100,
                "skip": 0,
                "limit": 10,
                "total_pages": 10,
                "page": 1
            }
        }