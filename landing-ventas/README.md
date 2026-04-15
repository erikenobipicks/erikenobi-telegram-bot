# Landing Ventas

Versión comercial larga de la landing de Erikenobi Picks.

Objetivo:

- explicar mejor la propuesta
- mostrar estructura comercial y pruebas
- servir como página de ventas más completa que la de Instagram

## Archivos

- `index.html`: página principal larga
- `styles.css`: diseño responsive
- `script.js`: animaciones, contadores y formulario
- `social-preview.svg`: preview social
- `data/landing-data.json`: estadísticas exportadas desde el bot

## Flujo recomendado

1. Crear un repo o despliegue específico para esta landing
2. Publicarla como sitio estático
3. Mantener `data/landing-data.json` actualizado con el script de exportación

## Datos automáticos desde el bot

El script principal escribe en:

- `landing-ventas/data/landing-data.json`

Y el script de sincronización trabaja contra:

- `landing-ventas`

## Cuándo usar esta landing

Usa esta carpeta si quieres una página más completa para tráfico templado o campañas con más contexto.
