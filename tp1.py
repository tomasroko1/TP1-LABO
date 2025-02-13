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

""" ANOTACIONES (ELIMINAR AL FINAL)

1) La base de datos 'padron_población' ES MALÍSIMA. No solo porque las cosas no están organizadas como corresponde,
si no porque usan un código de area identificador que por lo que estuve viendo NO ES el mismo que el codigo que tienen
'censos_culturales' y 'padron_poblacion' llamado 'Cod_loc' que parece ser el código de 'localidad censal'

2) El objetivo del informe es analizar la relación que hay entre #centros educativos y #centros culturales
    en cada PROVINCIA. Por lo tanto, el dato de 'departamento' no nos va a importar, tampoco el de 'localidad'

"""

# CHEQUEO QUE ESTO SEA UNA CLAVE
consultaSQL = dd.sql(
    """
    SELECT DISTINCT Nombre, Latitud, Longitud
    FROM centros_culturales
    """
    ).df()


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
     ##### EJECUTAR ESTA CELDA UNA SOLA VEZ!####
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