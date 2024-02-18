# Proyecto_BDD
## Proyecto de base de datos 1. Analisis de es investigar los datos presentados para responder a la siguiente
pregunta: Basado en el desempeño de los equipos y jugadores según este modelo, ¿a qué
equipo le apostaría usted? (debe de dar fundamentos basados en los datos)

## Instrucciones
1. Insertar psycopg2 de python con 'pip install psycopg2'
2. Crear una base de datos en postgres llamada 'proyecto1'
3. Editar el user con tu usuario y password con tu contraseña.
try:
    connection=psycopg2.connect(
        host = 'localhost',
        user = 'miusuario',
        password = 'micontraseña',
        database = 'proyecto1'
    )
4. Ejecutar el archivo python

![Mi imagen](./Data/diagrama.png)
