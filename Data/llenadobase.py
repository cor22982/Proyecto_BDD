import psycopg2
import csv
from datetime import datetime

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

def creacion_leagues(cursor,connection,archivo_csv):
    create_table_appearances = '''
    CREATE TABLE leagues (
        leagueID INT PRIMARY KEY,
        name VARCHAR(50),
        understatNotation VARCHAR(50)
    );
    '''
    print("Creacion de Leagues Exitosa")
    cursor.execute(create_table_appearances)
    columnas=["leagueID", "name","understatNotation"]
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
        insert_query = f'''INSERT INTO leagues ({", ".join(columnas)})
                           VALUES ({", ".join(["%s" for _ in range(len(columnas))])})'''
        cursor.executemany(insert_query, datos_a_insertar)    

def creacion_players(cursor,connection,archivo_csv):
    create_table_appearances = '''
    CREATE TABLE players (
        playerID INT PRIMARY KEY,
        name VARCHAR(50)
    );
    '''
    print("Creacion de players Exitosa")
    cursor.execute(create_table_appearances)
    columnas=["playerID", "name"]
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
        insert_query = f'''INSERT INTO players ({", ".join(columnas)})
                           VALUES ({", ".join(["%s" for _ in range(len(columnas))])})'''
        cursor.executemany(insert_query, datos_a_insertar)

def creacion_games(cursor, connection, archivo_csv):
    create_table_appearances = '''
    CREATE TABLE games (
        gameID INT PRIMARY KEY,
        leagueID INT,
        season INT,
        date TIMESTAMP,
        homeTeamID INT,
        awayTeamID INT,
        homeGoals INT,
        awayGoals INT,
        homeProbability FLOAT,
        drawProbability FLOAT,
        awayProbability FLOAT,
        homeGoalsHalfTime INT,
        awayGoalsHalfTime INT,
        B365H FLOAT,
        B365D FLOAT,
        B365A FLOAT,
        BWH FLOAT,
        BWD FLOAT,
        BWA FLOAT,
        IWH FLOAT,
        IWD FLOAT,
        IWA FLOAT,
        PSH FLOAT,
        PSD FLOAT,
        PSA FLOAT,
        WHH FLOAT,
        WHD FLOAT,
        WHA FLOAT,
        VCH FLOAT,
        VCD FLOAT,
        VCA FLOAT,
        PSCH FLOAT,
        PSCD FLOAT,
        PSCA FLOAT,
        foreign key (leagueID) references leagues(leagueID),
        foreign key (homeTeamID) references teams(teamID),
        foreign key (awayTeamID) references teams(teamID)
    );
    '''
    cursor.execute(create_table_appearances)
    print("Creacion de Games Exitosa")

    columnas = [
        "gameID", "leagueID", "season", "date", "homeTeamID", "awayTeamID", "homeGoals", "awayGoals",
        "homeProbability", "drawProbability", "awayProbability", "homeGoalsHalfTime", "awayGoalsHalfTime",
        "B365H", "B365D", "B365A", "BWH", "BWD", "BWA", "IWH", "IWD", "IWA", "PSH", "PSD", "PSA",
        "WHH", "WHD", "WHA", "VCH", "VCD", "VCA", "PSCH", "PSCD", "PSCA"
    ]

    with open(archivo_csv, 'r', newline='', encoding='utf-8') as archivo:
        lector_csv = csv.reader(archivo)
        next(lector_csv)  # Ignorar la primera fila si contiene encabezados

        # Preparar los datos para la inserción
        datos_a_insertar = []
        for fila in lector_csv:
            # Convertir la fecha y hora a formato TIMESTAMP
            fecha_hora_str = fila[3]  # Suponiendo que la fecha y hora están en la cuarta columna (0-indexed)
            fecha_hora = datetime.strptime(fecha_hora_str, '%m/%d/%Y %H:%M')
            fila[3] = fecha_hora.strftime('%Y-%m-%d %H:%M:%S')  # Convertir a formato TIMESTAMP

            # Suponiendo que tienes las columnas en el mismo orden que aparecen en el archivo CSV
            datos_fila = tuple(fila)
            datos_a_insertar.append(datos_fila)

        # Ejecutar la consulta de inserción para todas las filas
        insert_query = f'''INSERT INTO games ({", ".join(columnas)})
                           VALUES ({", ".join(["%s" for _ in range(len(columnas))])})'''
        cursor.executemany(insert_query, datos_a_insertar)

def creacion_appearances(cursor,connection,archivo_csv):
    create_table_appearances = '''
    CREATE TABLE appearances (
        gameID INT,
        playerID INT,
        goals INT,
        ownGoals INT,
        shots INT,
        xGoals FLOAT,
        xGoalsChain FLOAT,
        xGoalsBuildup FLOAT,
        assists INT,
        keyPasses INT,
        xAssists FLOAT,
        position VARCHAR(30),
        positionOrder INT,
        yellowCard INT,
        redCard INT,
        time INT,
        substituteIn INT,
        substituteOut INT,
        leagueID INT,
        foreign key (gameID) references games(gameID) ,
        foreign key (leagueID) references leagues(leagueID) ,
        foreign key (playerID) references players(playerID)
    );
    '''
    
    cursor.execute(create_table_appearances)
    columnas = [
        "gameID", "playerID", "goals", "ownGoals", "shots", "xGoals", "xGoalsChain",
        "xGoalsBuildup", "assists", "keyPasses", "xAssists", "position", "positionOrder",
        "yellowCard", "redCard", "time", "substituteIn", "substituteOut", "leagueID"
    ]

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
        insert_query = f'''INSERT INTO appearances ({", ".join(columnas)})
                           VALUES ({", ".join(["%s" for _ in range(len(columnas))])})'''
        cursor.executemany(insert_query, datos_a_insertar)
    print("Creacion de appearances")

def creacion_teamstats(cursor, connection, archivo_csv):
    create_table_appearances = '''
    CREATE TABLE teamstats (
        gameID INT,
        teamID INT,
        season INT,
        date TIMESTAMP,
        location VARCHAR(1),
        goals INT,
        xGoals FLOAT,
        shots INT,
        shotsOnTarget INT,
        deep FLOAT,
        ppda FLOAT,
        fouls INT,
        corners INT,
        yellowCards INT,
        redCards INT,
        result VARCHAR(1),
        foreign key (gameID) references games(gameID),
        foreign key (teamID) references teams(teamID)
    );
    '''
    cursor.execute(create_table_appearances)
    
    columnas = [
        "gameID", "teamID", "season", "date", "location", "goals", "xGoals",
        "shots", "shotsOnTarget", "deep", "ppda", "fouls", "corners",
        "yellowCards", "redCards", "result"
    ]


    with open(archivo_csv, 'r', newline='', encoding='utf-8') as archivo:
        lector_csv = csv.reader(archivo)
        next(lector_csv)  # Ignorar la primera fila si contiene encabezados

        # Preparar los datos para la inserción
        datos_a_insertar = []
        for fila in lector_csv:
            # Convertir la fecha y hora a formato TIMESTAMP
            fecha_hora_str = fila[3]  # Suponiendo que la fecha y hora están en la cuarta columna (0-indexed)
            fecha_hora = datetime.strptime(fecha_hora_str, '%m/%d/%Y %H:%M')
            fila[3] = fecha_hora.strftime('%Y-%m-%d %H:%M:%S')  # Convertir a formato TIMESTAMP

            # Suponiendo que tienes las columnas en el mismo orden que aparecen en el archivo CSV
            datos_fila = tuple(fila)
            datos_a_insertar.append(datos_fila)

        # Ejecutar la consulta de inserción para todas las filas
        insert_query = f'''INSERT INTO teamstats ({", ".join(columnas)})
                           VALUES ({", ".join(["%s" for _ in range(len(columnas))])})'''
        cursor.executemany(insert_query, datos_a_insertar)
    print("Creacion de Teamstats Exitosa")

def creacion_shots(cursor, connection, archivo_csv):
    create_table_appearances = '''
    CREATE TABLE shots (
        gameID INT,
        shooterID INT,
        assisterID INT,
        minute INT,
        situation VARCHAR(50),
        lastAction VARCHAR(50),
        shotType VARCHAR(50),
        shotResult VARCHAR(50),
        xGoal FLOAT,
        positionX FLOAT,
        positionY FLOAT,
        foreign key (gameID) references games(gameID),
        foreign key (shooterID) references players(playerID),
        foreign key (assisterID) references players(playerID)
    );
    '''
    cursor.execute(create_table_appearances)
    columnas = [
        "gameID", "shooterID", "assisterID", "minute", "situation", "lastAction",
        "shotType", "shotResult", "xGoal", "positionX", "positionY"
    ]
    with open(archivo_csv, 'r', newline='', encoding='utf-8') as archivo:
        lector_csv = csv.reader(archivo)
        next(lector_csv)  # Ignorar la primera fila si contiene encabezados

        # Preparar los datos para la inserción
        datos_a_insertar = []
        for fila in lector_csv:
            # Suponiendo que tienes las columnas en el mismo orden que aparecen en el archivo CSV
            gameID, shooterID, assisterID, minute, situation, lastAction, shotType, shotResult, xGoal, positionX, positionY = fila
            # Convertir 'NA' o vacío a None (NULL en SQL)
            assisterID = int(assisterID) if assisterID.isdigit() else None
            datos_fila = (gameID, shooterID, assisterID, minute, situation, lastAction, shotType, shotResult, xGoal, positionX, positionY)
            datos_a_insertar.append(datos_fila)

        # Ejecutar la consulta de inserción para todas las filas
        insert_query = f'''INSERT INTO shots ({", ".join(columnas)})
                           VALUES ({", ".join(["%s" for _ in range(len(columnas))])})'''
        cursor.executemany(insert_query, datos_a_insertar)
    print("Creacion de Shots Exitosa")

try:
    connection=psycopg2.connect(
        host = 'localhost',
        user = 'postgres',
        password = 'medueleperomeex1tA',
        database = 'proyecto1'
    )
    
    #CREACION TABLA teams
    cursor = connection.cursor()
    creacion_teams(cursor,connection,'./Data/teams.csv')
    creacion_leagues(cursor,connection,'./Data/leagues.csv')
    creacion_players(cursor,connection,'./Data/players.csv')
    connection.commit()
    creacion_games(cursor,connection,'./Data/games.csv')
    creacion_appearances(cursor,connection,'./Data/appearances.csv')
    creacion_teamstats(cursor,connection,'./Data/teamstats.csv')
    creacion_shots(cursor,connection,'./Data/shots.csv')
    connection.commit()

except Exception as ex:
    print(ex)

finally:
    connection.close()