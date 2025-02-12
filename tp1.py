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

