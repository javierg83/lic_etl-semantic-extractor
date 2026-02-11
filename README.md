# LIC ETL Semantic Extractor

Este proyecto es un extractor semántico diseñado para procesar licitaciones, enfocándose en la extracción de información financiera e ítems.

## Arquitectura

El proyecto utiliza **LangGraph** para orquestar el flujo de extracción como un grafo de estados.

### Flujo del Grafo
1. **Load Data**: Carga el contexto de la licitación.
2. **Extraction**: Ejecuta extracciones en paralelo:
   - **Finanzas**: Presupuestos, garantías, monedas.
   - **Items**: Lista de productos o servicios solicitados.
3. **Save**: Persiste los resultados (actualmente simulación).

## Estructura

- `src/graph`: Definición del grafo y el estado.
- `src/nodes`: Lógica de cada paso (nodo) del proceso.
- `src/main.py`: Script para ejecución local y pruebas.
- `src/worker.py`: Worker para procesamiento asíncrono con Redis.

## Configuración

Crear un archivo `.env` basado en las variables requeridas en `src/config.py`:
- `REDIS_HOST`, `REDIS_PORT`, etc.
- `OPENAI_API_KEY` (para futuras implementaciones de LLM).

## Ejecución

### Local
```bash
python -m src.main
```

### Worker (Redis)
```bash
python -m src.worker
```
