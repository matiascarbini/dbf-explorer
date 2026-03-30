# DBF Explorer

Visor web simple para carpetas con archivos `.DBF`, pensado para correr dentro de Docker y montar una carpeta local en modo solo lectura.

## Qué hace

- Lista todos los archivos `.DBF` de la carpeta montada.
- Permite abrir cualquier tabla y ver sus registros.
- Tiene paginación para soportar archivos grandes.
- Funciona sin base de datos adicional.

## Cómo usarlo

1. Indicá la carpeta local que contiene tus `.DBF`.
2. Levantá el contenedor:

```bash
DBF_SOURCE_DIR=/ruta/a/tu/carpeta/con/dbf docker compose up --build
```

3. Abrí el navegador en:

```text
http://localhost:8000
```

## Variables útiles

- `DBF_SOURCE_DIR`: carpeta local a montar dentro del contenedor.
- `DBF_ENCODING`: codificación de texto para leer los DBF. Por defecto `latin1`.
- `DBF_PAGE_SIZE`: cantidad de filas por página. Por defecto `100`.

## Ejemplo real

Si tus archivos están en:

```text
/home/matias/DESARROLLO/TEST/APRENDICES/bck-socios/socios/bases
```

podés ejecutar:

```bash
DBF_SOURCE_DIR=/home/matias/DESARROLLO/TEST/APRENDICES/bck-socios/socios/bases docker compose up --build
```

## Notas

- El contenedor monta la carpeta en `/data/dbf`.
- El montaje es `read-only` para no tocar tus archivos originales.
- Si algunos textos se ven mal, probá cambiar `DBF_ENCODING`, por ejemplo a `cp437` o `cp850`.
