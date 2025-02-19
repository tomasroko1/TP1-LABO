# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 21:52:13 2025

@author: tomas
"""

#     Importamos librerías

import pandas as pd
import duckdb as dd

#%%   Cargamos los archivos

establecimientos_educativos = pd.read_excel('2022_padron_oficial_establecimientos_educativos.xlsx')

centros_culturales = pd.read_csv('centros_culturales.csv')

padron_poblacion = pd.read_excel('padron_poblacion.xlsX')

#%% 
""" 
                                 ###########################################
                                 #####      Procesamiento de Datos      ####
                                 ###########################################


    'En las siguientes 3 celdas, nos enfoncamos en procesar los datos para que luego estén disponibles para trabajar'
"""
#%%

"""--------------------------------------Métrica 1 - Centros Culturales---------------------------------------------"""

# Elimino el espacio vacío en 'Mail '
centros_culturales.columns = centros_culturales.columns.str.strip()

# METRICA PARA LA CALIDAD DEL DATO 'Mail' EN LA TABLA 'centros_culturales'
metrica01 = dd.sql(
    """
    SELECT COUNT(*) * 100 / (SELECT COUNT(*) FROM centros_culturales) AS porcentaje_nulls
    FROM centros_culturales
    WHERE Mail IS NULL
    OR (Mail IN ('-', 's/d'))
    """
    ).df()
"""-----------------------------------------------------------------------------------------------------------------"""

# Nos quedamos con las columnas de interés y cambiamos algunos tipos de datos
centros_culturales = centros_culturales.iloc[:, [0, 1, 2, 5, 6, 8, 14, 17, 18, 22]]
centros_culturales['Longitud'] = centros_culturales['Longitud'].astype(float)


#%%

"""----------------------------------Métrica 2 - Establecimientos Educativos----------------------------------------"""
# Asignar la fila 5 como nombres de columna
establecimientos_educativos.columns = establecimientos_educativos.iloc[5]

# Eliminar las primeras 6 filas para comenzar desde la fila 6
establecimientos_educativos = establecimientos_educativos.iloc[6:].reset_index(drop=True)

# Pasamos a str por si es null, y dsp le sacamos los espacios vacíos para evitar problemas
establecimientos_educativos["Código de área"] = establecimientos_educativos["Código de área"].astype(str).str.strip()
establecimientos_educativos["Teléfono"] = establecimientos_educativos["Teléfono"].astype(str).str.strip()

# Sacamos guiones con replace
establecimientos_educativos["Teléfono"] = establecimientos_educativos["Teléfono"].str.replace("-", "", regex=False)

# Filtramos solo lo números válidos
nrosvalidos = dd.sql(
    """
    SELECT Teléfono
    FROM establecimientos_educativos
    WHERE LENGTH(Teléfono) > 5
    AND Teléfono NOT IN ('0', '1', '-', 'sn', 's/n', '', ' ', '.', 'ss', 's/inf.', 'S/Inf.',
                         'SN', 'S/N', 's/inf', 'ooooooo', 'no tiene', 'no posee', 'None', 'No posee',
                         '999999', '9999999', '99999999', '999999999', 'CAB.PUB.80260',
                         'NO POSEE', 
                         'RED OFICIAL 978',
                         'SE CREA POR RESOL. 1707/2022 MECCyT FECHA:27/04/22',
                         'SE CREA POR RESOL. N°1790/2021 MECCyT FECHA:10/12/21')
    AND Teléfono NOT LIKE '00%'  -- Sacámos los nros que empiezan con 00
    AND Teléfono NOT LIKE '%*'
    AND Teléfono NOT LIKE '*%'
    AND Teléfono IS NOT NULL
    """
).df()

# METRICA PARA LA CALIDAD DEL DATO 'Teléfono' EN LA TABLA 'establecimientos_educativos'
# CDAD DE ESCUELAS SIN NUMERO DE TELEFONO VÁLIDO



"""--------------------------------------Métrica 2------------------------------------------------------------------"""
metrica02 = dd.sql(
    """
    SELECT 100 - (COUNT(*) * 100 / (SELECT COUNT(*) FROM establecimientos_educativos)) AS porcentaje_invalidos
    FROM nrosvalidos 
    """
    ).df()

# Renombro la ultima columna  
establecimientos_educativos.rename(columns={establecimientos_educativos.columns[-1]: "Servicios complementarios"}, inplace=True)

# Agrego la columna Area a un nuevo dataframe "informacion_escuelas"
establecimientos_educativos['Código de localidad'] = establecimientos_educativos['Código de localidad'].astype(int)
establecimientos_educativos.rename(columns = {'Código de localidad':'cod_loc'}, inplace = True)

# Me quedo con las columnas que quiero
establecimientos_educativos = establecimientos_educativos.iloc[:, [0, 1, 2, 9, 11, 12, 20, 21, 22, 23, 24]]

# Reemplacemos los vacíos en los niveles educativos por 0

cols = [
    "Nivel inicial - Jardín maternal",
    "Nivel inicial - Jardín de infantes",
    "Primario",
    "Secundario",
    "Secundario - INET"
]

establecimientos_educativos.loc[:, cols] = (
    establecimientos_educativos.loc[:, cols]
    .replace({' ': 0, '': 0})
    .fillna(0)
    .astype(int)
)
#%%

"""-------------------------------------Padrón Población------------------------------------------------------------"""

def calcular_largo_areas():
    # Lista que va a contener el largo de cada tabla Area (nombre del área y cantidad de filas)
    largo_areas = []

    i = 0
    total_filas = len(padron_poblacion)

    # Recorremos el dataframe paron_poblacion saltando de a 80 filas ( Saltamos de a 80 porque no hay Áreas con mas de 80 registros)
    while i < total_filas:
        # Buscamos la aparicion de "AREA #" para determinar el largo de la tabla
        
        sub_df = padron_poblacion.iloc[i:min(i + 80, total_filas)]  
        area_fila = sub_df[sub_df["Unnamed: 1"].astype(str).str.startswith("AREA #", na=False)]

        if not area_fila.empty:
            # Obtener el índice de la fila del área
            area_index = area_fila.index[0]
            codigo_area = padron_poblacion.loc[area_index, "Unnamed: 1"]
            nombre_area = padron_poblacion.loc[area_index, "Unnamed: 2"]

            # Contar filas hasta el  próximo "AREA #"
            fila_actual = area_index + 2  # Saltamos 2 filas después del área (Todas las tablas arrancan dos filas despues del 'AREA #')
            contador = -3 # Corrijo un desface que me produce la función al final (por eso arranco de -3, pues todas las areas tenían 3 mas que el largo real)

            while fila_actual < total_filas:
                if isinstance(padron_poblacion.loc[fila_actual, "Unnamed: 1"], str) and "AREA #" in padron_poblacion.loc[fila_actual, "Unnamed: 1"]:
                    break  # Paramos de contar si encontramos un área
                contador += 1
                fila_actual += 1

            largo_areas.append((codigo_area, nombre_area, contador))
            i = fila_actual
    

    # A ESTO LO HAGO PORQUE SE CONFUNDE EL PROGRAMA CON EL FINAL DE TODO (porque al final esta el resumen)
    largo_areas[len(largo_areas)-1] = tuple(('AREA # 94015', 'Ushuaia', 102))

    return largo_areas

areas_info = calcular_largo_areas()

def extraer_bloques_variable_longitudes(
    df,
    areas_info,
    start_index=15,  # Arranca en df.iloc[15] (0-based)
    skip_size=5
):
    """
    df: DataFrame original.
    areas_info: lista de tuplas (nombre_area, largo), en orden. 
                Ej: [("AREA # 02007", 110), ("AREA # 02014", 108), ...]
    start_index: índice 0-based donde empezar. Si queremos que la 'primera fila' 
                 sea la 15 contando desde 0, ponemos 15.
    skip_size: cuántas filas saltear tras cada bloque.

    Devuelve: DataFrame concatenado de todos los bloques, 
              con una columna 'Area' que indica el nombre de área.
    """
    # Asegurar que el índice sea 0..N consecutivo
    df = df.reset_index(drop=True)
    
    current_index = start_index
    blocks = []
    
    for codigo_area, nombre_area, length in areas_info:
        # Verifica si hay suficientes filas para tomar 'length' desde current_index
        if current_index + length > len(df):
            # No hay más filas suficientes, cortamos aquí
            break
        
        # Extraer 'length' filas
        bloque = df.iloc[current_index : current_index + length].copy()
        
        # Asignamos el nombre y el código del área
        bloque['Area'] = codigo_area
        bloque['Descripción'] = nombre_area
        
        
        
        blocks.append(bloque)
        
        # Avanzar el puntero length + skip_size
        current_index += length + skip_size
    
    # Unir todos los bloques en un DataFrame
    if blocks:
        return pd.concat(blocks, ignore_index=True)
    else:
        return pd.DataFrame()


padron_poblacion = extraer_bloques_variable_longitudes(padron_poblacion, areas_info)

# saco la 1ra columna de nulls
padron_poblacion.drop(columns=['CEPAL/CELADE Redatam+SP 01/30/2025'], inplace=True)

# renomnramos las columnas 
padron_poblacion.rename(columns={
    'Unnamed: 1': 'Edad',
    'Unnamed: 2': 'Casos',
    'Unnamed: 3': '%',
    'Unnamed: 4': 'Acumulado %'
}, inplace=True)


# Saco los 'AREA #' y lo pasamos a int, también pasamos 'casos' y 'edad' a int
padron_poblacion['Area'] = padron_poblacion['Area'].str.replace(r'\D', '', regex=True).astype(int)
padron_poblacion['Casos'] = padron_poblacion['Casos'].astype(int)
padron_poblacion['Edad'] = padron_poblacion['Edad'].astype(int)

# Nos quedamos con los datos de interés
padron_poblacion = padron_poblacion.iloc[:, [0, 1, 4, 5]]

"""-----------------------------------------------------------------------------------------------------------------"""
#%%
""" 
                                     ###########################
                                     #####  Normalización  ##### 
                                     ###########################
                                     
    'En las siguientes 3 celdas normalizamos los tres datasets de la manera explicada a detalle en el informe'
"""
#%%
"""---------------------------------------Centros Culturales--------------------------------------------------------"""

ubicacion = centros_culturales.iloc[:, [7, 8, 0]]

localidad = centros_culturales.iloc[:, [0, 1, 2]]

provincia = centros_culturales.iloc[:, [1, 3]]

departamento = centros_culturales.iloc[:, [2, 1, 4]]

centros_culturales = centros_culturales.iloc[:, [7, 8, 5, 6, 9]]

#%%
"""---------------------------------------Padrón Población----------------------------------------------------------"""

area_censal = padron_poblacion.iloc[:, [2, 3]]

padron_poblacion = padron_poblacion.iloc[:, [2, 0, 1]]

#%%
"""----------------------------------Establecimientos Educativos----------------------------------------------------"""

localidad_escuelas = establecimientos_educativos.iloc[:, [3, 0, 4]]

establecimientos_educativos = establecimientos_educativos.iloc[:, [1, 2, 3, 5, 6, 7, 8, 9, 10]]

#%% 

""" 
                                     ###########################
                                     #####  Consultas SQL  ##### 
                                     ###########################
"""

# i) Jadrín maternal hasta los 2 años (VER LO DE LOS 45 DIAS)
#    Jardín de infantes de 3 a 5 


# LA QUE VA ES RELACIONAR EL CODIGO DE LOCALIDAD CON EL AREA

consulta1 = dd.sql(
            """
    SELECT 
        Jurisdicción AS Provincia,
        Departamento,
        Area,
        SUM(CASE WHEN "Nivel inicial - Jardín maternal" = 1 OR "Nivel inicial - Jardín de infantes" = 1 THEN 1 ELSE 0 END) AS Jardines,
        SUM(CASE WHEN "Primario" = 1 THEN 1 ELSE 0 END) AS Primarios,
        SUM(CASE WHEN "Secundario" = 1 OR "Secundario - INET" = 1 THEN 1 ELSE 0 END) AS Secundarios
        
    FROM establecimientos_educativos
    GROUP BY Area, Departamento, Jurisdicción
            """).df()
            
# Area y poblaciones estudiantiles de c/area
consulta2 = dd.sql(
            """
    SELECT 
        Area,
        SUM(CASE WHEN Edad BETWEEN 0 AND 5 THEN Casos ELSE 0 END) AS 'poblacion_jardin',
        SUM(CASE WHEN Edad BETWEEN 6 AND 11 THEN Casos ELSE 0 END) AS 'poblacion_primaria',
        SUM(CASE WHEN Edad BETWEEN 12 AND 18 THEN Casos ELSE 0 END) AS 'poblacion_secundaria'
        
    FROM padron_poblacion
    GROUP BY Area
    
            """).df()

ejercicio_i = dd.sql("""
                     SELECT Provincia, Departamento,
                     Jardines, poblacion_jardin AS'Población Jardín',
                     Primarios, poblacion_primaria AS 'Población Primaria',
                     Secundarios, poblacion_secundaria AS 'Población Secundaria'
                     FROM consulta1 AS c1
                     JOIN consulta2 AS c2
                     ON c1.Area = c2.Area 
                     """).df()
                     
sub_consulta_ejercicio_ii = dd.sql("""
                    SELECT *
                    FROM centros_culturales
                    WHERE Capacidad > 100.0
                    """).df()

ejercicio_ii = dd.sql("""
                    SELECT Provincia, Departamento, COUNT(*) AS Cantidad
                    FROM sub_consulta_ejercicio_ii
                    GROUP BY Provincia, Departamento
                    ORDER BY Provincia ASC, Cantidad DESC
                    """).df()

