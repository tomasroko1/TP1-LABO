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

padron_poblacion = pd.read_excel('padron_poblacion.xlsx')

#%%

""" 
     El objetivo del informe es analizar la relación que hay entre #centros educativos y #centros culturales
    en cada PROVINCIA. Por lo tanto, el dato de 'departamento' no nos va a importar, tampoco el de 'localidad'

"""

# CHEQUEO QUE ESTO SEA UNA CLAVE
consultaSQL = dd.sql(
    """
    SELECT DISTINCT Nombre, Latitud, Longitud
    FROM centros_culturales
    """
    ).df()

#%% 

""" 
     ###########################################
     #####      Procesamiento de Datos      ####
     ###########################################
"""

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

#%% Quiero acomodar la tabla "establecimientos_educativos"        

""" 
     ###########################################
     ##### Ejecutar esta celda una sola vez ####
     ###########################################
"""

# Asignar la fila 5 como nombres de columna
establecimientos_educativos.columns = establecimientos_educativos.iloc[5]

# Eliminar las primeras 6 filas para comenzar desde la fila 6
establecimientos_educativos = establecimientos_educativos.iloc[6:].reset_index(drop=True)

# Renombro la ultima columna  
establecimientos_educativos.rename(columns={establecimientos_educativos.columns[-1]: "Servicios complementarios"}, inplace=True)

#%% Quiero ver cuántos números de teléfono son válidos, y cuántos tienen nro de interno

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

metrica02 = dd.sql(
    """
    SELECT 100 - (COUNT(*) * 100 / (SELECT COUNT(*) FROM establecimientos_educativos)) AS proporcion
    FROM nrosvalidos 
    """
    ).df()


#%%  

""" 
     ###########################################
     #####        Padrón Población         #####
     ###########################################
"""

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
            area_name = padron_poblacion.loc[area_index, "Unnamed: 1"]

            # Contar filas hasta el  próximo "AREA #"
            fila_actual = area_index + 2  # Saltamos 2 filas después del área (Todas las tablas arrancan dos filas despues del 'AREA #')
            contador = -3 # Corrijo un desface que me produce la función al final (por eso arranco de -3, pues todas las areas tenían 3 mas que el largo real)

            while fila_actual < total_filas:
                if isinstance(padron_poblacion.loc[fila_actual, "Unnamed: 1"], str) and "AREA #" in padron_poblacion.loc[fila_actual, "Unnamed: 1"]:
                    break  # Paramos de contar si encontramos un área
                contador += 1
                fila_actual += 1

            largo_areas.append((area_name, contador))
            i = fila_actual
    

    # A ESTO LO HAGO PORQUE SE CONFUNDE EL PROGRAMA CON EL FINAL DE TODO (porque al final esta el resumen)
    largo_areas[len(largo_areas)-1] = tuple(('AREA # 94015', 102))

    return largo_areas

areas_info = calcular_largo_areas()

#%%

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
    
    for area_label, length in areas_info:
        # Verifica si hay suficientes filas para tomar 'length' desde current_index
        if current_index + length > len(df):
            # No hay más filas suficientes, cortamos aquí
            break
        
        # Extraer 'length' filas
        bloque = df.iloc[current_index : current_index + length].copy()
        
        # Asignar el nombre de área
        bloque['Area'] = area_label
        
        blocks.append(bloque)
        
        # Avanzar el puntero length + skip_size
        current_index += length + skip_size
    
    # Unir todos los bloques en un DataFrame
    if blocks:
        return pd.concat(blocks, ignore_index=True)
    else:
        return pd.DataFrame()


df_filtrado = extraer_bloques_variable_longitudes(padron_poblacion, areas_info)

#%%

# saco la 1ra columna de nulls
df_filtrado.drop(columns=['CEPAL/CELADE Redatam+SP 01/30/2025'], inplace=True)

# renomnramos las columnas 
df_filtrado.rename(columns={
    'Unnamed: 1': 'Edad',
    'Unnamed: 2': 'Casos',
    'Unnamed: 3': '%',
    'Unnamed: 4': 'Acumulado %'
}, inplace=True)


# Saco los 'AREA #' y lo pasamos a int
df_filtrado['Area'] = df_filtrado['Area'].str.replace(r'\D', '', regex=True).astype(int)

#%%


