# Reglas para agentes IA — DBF Explorer

## Qué es este proyecto

Visor web local para navegar archivos `.DBF`. Corre en Docker, monta una carpeta local en modo solo lectura y expone una interfaz web en `http://localhost:8000`. No tiene base de datos propia (usa SQLite solo como caché interna). Es una herramienta de uso personal, no se deploya en servidores externos.

## Stack

- **Backend**: Python 3.12, Flask, dbfread, gunicorn
- **Frontend**: HTML + CSS plano, sin frameworks JS
- **Contenedor**: Docker + docker-compose
- **Tareas**: `make up / down / restart / logs`

## Idioma

Todo en **español**: nombres de variables, funciones, comentarios, mensajes al usuario y commits. El código Python existente ya sigue esta convención.

## Tests

El proyecto no tiene tests y no se planea agregarlos. No crear archivos de test ni sugerir frameworks de testing.

## Dependencias

Antes de agregar cualquier librería a `requirements.txt`, **preguntar al usuario y esperar confirmación**. No asumir que cualquier paquete está aprobado.

## Zonas sensibles — no tocar sin consultar

Los siguientes bloques en [app/app.py](app/app.py) son específicos del dominio de datos con el que trabaja el usuario y **no deben modificarse sin preguntar**:

- `TABLE_HINTS`: descripciones de tablas propias del dominio (mutual/obra social).
- `TABLE_EXPORT_PRESETS`: configuración de exportaciones JSON personalizadas por tabla.

Cualquier cambio en estos diccionarios requiere aprobación explícita del usuario.

## Frontend

Libertad total para modificar templates HTML y CSS en [app/templates/](app/templates/) y [app/static/](app/static/). Se puede reescribir, reorganizar o mejorar sin restricciones. Mantener HTML/CSS plano salvo que el usuario pida otra cosa.

## Seguridad básica (aunque es local)

- Los archivos DBF se montan **en modo solo lectura** (`ro`). Nunca proponer código que intente escribir en `/data/dbf`.
- Las rutas SQL ya tienen validación contra escritura (`READ_ONLY_SQL`, `FORBIDDEN_SQL`). No debilitar esas validaciones.

## Flujo de trabajo

- Leé el código antes de proponer cambios.
- No agregar manejo de errores, abstracciones ni features que no se pidieron.
- Si algo no está claro sobre los datos del dominio (nombres de tablas, campos DBF, lógica de negocio), preguntar antes de inventar.
