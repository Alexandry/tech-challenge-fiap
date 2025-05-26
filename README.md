# Tech Challenge

Este é o projeto **Tech Challenge**, desenvolvido como parte do primeiro módulo da pós-graduação em Machine Learning. O objetivo do projeto é criar uma aplicação web que consome dados de tabelas HTML do site da Embrapa e os disponibiliza por meio de uma API REST moderna, documentada e pronta para uso em integrações e análises.

---

## O que o projeto resolve

O projeto resolve o problema de acesso automatizado e estruturado aos dados públicos de produção, processamento, comercialização, importação e exportação de produtos vitivinícolas do Brasil, originalmente disponíveis apenas em tabelas HTML no site da Embrapa. Ele transforma esses dados em endpoints RESTful, com filtros, paginação e cache, facilitando o consumo por aplicações, dashboards, cientistas de dados, pesquisadores e empresas do setor.

---

## Quem pode usar a API

- **Pesquisadores**: Para análises de dados do setor vitivinícola brasileiro.
- **Empresas**: Para integrar dados de produção, importação e exportação em seus sistemas.
- **Desenvolvedores**: Para construir aplicações, dashboards e automações.
- **Estudantes**: Para estudos de ciência de dados, APIs e integração de dados públicos.

---

## Funcionalidades

A aplicação oferece as seguintes rotas para acessar os dados:

- **`/api/produtos/producao`**: Dados de produção de produtos.
- **`/api/produtos/processamento`**: Dados de processamento de produtos.
- **`/api/produtos/comercializacao`**: Dados de comercialização de produtos.
- **`/api/dados/importacao`**: Dados de importação, incluindo país, quantidade e valor, com filtros e paginação.
- **`/api/dados/exportacao`**: Dados de exportação, incluindo país, quantidade e valor, com filtros e paginação.

**Recursos extras:**
- Filtros por país, quantidade mínima/máxima, categoria (com busca parcial/"like").
- Paginação em todos os endpoints principais.
- Cache automático dos resultados no MongoDB para maior performance.
- Documentação automática via Swagger/OpenAPI.

---

## Estrutura do Projeto

```
├── .devcontainer/
│   ├── devcontainer.json
│   └── Dockerfile
├── .env
├── .gitignore
├── README.md
├── requirements.txt
├── main.py
├── app/
│   ├── __init__.py
│   ├── db/
│   │   ├── __init__.py
│   │   └── mongo.py
│   ├── model/
│   │   ├── __init__.py
│   │   ├── produto.py
│   │   └── dados_comerciais.py
│   ├── routes/
│   │   ├── __init__.py
│   │   ├── produto_routes.py
│   │   ├── importacao_routes.py
│   │   └── exportacao_routes.py
│   └── services/
│       ├── __init__.py
│       ├── embrapa_service.py
│       └── cache_service.py
```

---

## Tecnologias Utilizadas

- **Python 3.12**
- **FastAPI**: Framework web moderno e rápido para APIs.
- **Uvicorn**: Servidor ASGI para rodar a aplicação FastAPI.
- **MongoDB**: Banco de dados NoSQL para persistência e cache.
- **Pymongo**: Driver Python para MongoDB.
- **BeautifulSoup**: Extração de dados de HTML.
- **Requests**: Requisições HTTP.
- **python-dotenv**: Gerenciamento de variáveis de ambiente.
- **Docker/Dev Containers**: Ambiente de desenvolvimento isolado e reprodutível.
- **VS Code + Extensão MongoDB**: Para gerenciamento visual do banco.

---

## Como Executar

1. **Clone o repositório:**
   ```bash
   git clone <URL_DO_REPOSITORIO>
   cd tech-challenge-fiap
   ```

2. **Abra no VS Code e use Dev Containers:**
   - F1 → Dev Containers: Rebuild and Reopen in Container

3. **Configure o arquivo `.env`:**
   - Exemplo:
     ```
     MONGO_URI=mongodb+srv://<usuario>:<senha>@<cluster>.mongodb.net/?retryWrites=true&w=majority
     MONGO_DB_NAME=tech-challenge
     ```

4. **Execute a aplicação:**
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8080 --reload
   ```

5. **Acesse a documentação interativa:**
   ```bash
   $BROWSER http://localhost:8080/docs
   ```

---

## Exemplos de Uso

- **Paginação:**  
  `/api/dados/importacao?page=2&size=20`
- **Filtro por país:**  
  `/api/dados/exportacao?pais=Alemanha`
- **Filtro por quantidade:**  
  `/api/dados/importacao?qtd_min=10000&qtd_max=50000`
- **Filtro parcial de categoria:**  
  `/api/produtos/producao?categoria=Mesa`

---

## Contribuição

Thiago Ramos, Lucas Caique e Eduardo Barbosa

---

## Licença

Este projeto é acadêmico e para fins educacionais.