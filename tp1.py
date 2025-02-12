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

# Concatenar código de área y teléfono, si el código de área no es nulo
establecimientos_educativos["Telefono_Completo"] = (
    establecimientos_educativos["Código de área"].astype(str) +
    establecimientos_educativos["Teléfono"].astype(str)
)

# Remover espacios extra
establecimientos_educativos["Telefono_Completo"] = (
    establecimientos_educativos["Telefono_Completo"].str.strip()
)



nrosvalidos = dd.sql(
    """
    SELECT Telefono_Completo
    FROM establecimientos_educativos
    WHERE LENGTH('Teléfono_Completo')  > 9 
    AND "Telefono_Completo" NOT IN ('000', ' 00', '0', '1', '00', '-', 'sn', 's/n', '', '  -', 'ss', 's/inf.',
                           'SN', 's/inf', 'ooooooo', 'no tiene', 'no posee',
                           'SE CREA POR RESOL. 1707/2022 MECCyT FECHA:27/04/22',
                           'SE CREA POR RESOL. N°1790/2021 MECCyT FECHA:10/12/21',
                           'SECUNDARIA 4020240 / PRIMARIA 4056926')
    AND Telefono_Completo NOT LIKE '0%'  -- Excluye cualquier número que comience con 0
    AND Telefono_Completo IS NOT NULL
    """
    ).df()




