import psycopg2
import csv

def creacion_teams(cursor,connection,archivo_csv):
    create_table_appearances = '''
    CREATE TABLE teams (
        teamID INT PRIMARY KEY,
        name VARCHAR(50)
    );
    '''
    print("Creacion de Teams Exitosa")
    cursor.execute(create_table_appearances)
    columnas=["teamID", "name"]
    with open(archivo_csv, 'r', newline='', encoding='utf-8') as archivo:
        lector_csv = csv.reader(archivo)
        next(lector_csv)  # Ignorar la primera fila si contiene encabezados

        # Preparar los datos para la inserción
        datos_a_insertar = []
        for fila in lector_csv:
            # Suponiendo que tienes las columnas en el mismo orden que aparecen en el archivo CSV
            datos_fila = tuple(fila)
            datos_a_insertar.append(datos_fila)

        # Ejecutar la consulta de inserción para todas las filas
        insert_query = f'''INSERT INTO teams ({", ".join(columnas)})
                           VALUES ({", ".join(["%s" for _ in range(len(columnas))])})'''
        cursor.executemany(insert_query, datos_a_insertar)    

try:
    connection=psycopg2.connect(
        host = 'localhost',
        user = 'postgres',
        password = 'medueleperomeex1tA',
        database = 'proyecto'
    )
    
    #CREACION TABLA teams
    cursor = connection.cursor()
    creacion_teams(cursor,connection,'./Data/teams.csv')
    connection.commit()
    #Ahora vamos a crear los datos. 

except Exception as ex:
    print(ex)

finally:
    connection.close()