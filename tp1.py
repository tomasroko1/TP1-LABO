# -*- coding: utf-8 -*-
"""
    11/02/2025 

Trabajo práctico 1 - Laboratorio de datos - Verano 2025

Integrantes : Roko, Tomas
              Badii, Marina
              Ballera, Alexander 
"""

#     Importamos librerías

import pandas as pd
import duckdb as dd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score

#%%   Cargamos los archivos

carpeta = ''

establecimientos_educativos = pd.read_excel(carpeta + '2022_padron_oficial_establecimientos_educativos.xlsx')

centros_culturales = pd.read_csv(carpeta + 'centros_culturales.csv')

padron_poblacion = pd.read_excel(carpeta + 'padron_poblacion.xlsx')

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

# Nos quedamos con las columnas de interés y cambiamos algunos tipos de datos
centros_culturales = centros_culturales.iloc[:, [0, 1, 2, 5, 6, 8, 14, 17, 18, 22]]
centros_culturales['Longitud'] = centros_culturales['Longitud'].astype(float)
centros_culturales["Provincia"] = centros_culturales["Provincia"].str.upper()
centros_culturales["Departamento"] = centros_culturales["Departamento"].str.upper()

"""
nuevo
"""

provincia = centros_culturales.iloc[:, [1, 3]].drop_duplicates()

# Función para extraer el código de departamento a partir del codigo de localidad
def extraer_id_depto(cod_loc):
    cod_loc = str(cod_loc)  # Convertir a string para evitar errores
    return cod_loc[:5] if len(cod_loc) == 8 else cod_loc[:4]

centros_culturales["ID_DEPTO"] = centros_culturales["Cod_Loc"].apply(extraer_id_depto).astype(int)

centros_culturales = centros_culturales[['ID_DEPTO', 'Nombre', 'Capacidad', 'Mail']].reset_index().rename(columns={'index': 'ID_CC'})

# Separamos los mails
cc_mails_simples =  dd.sql("""
                    SELECT ID_CC, Nombre, Mail
                    FROM centros_culturales
                    WHERE Mail IS NOT NULL
                    AND Mail NOT IN ('s/d', '', '-', '/', '//')
                    AND Mail not like '%@%@%'
                    """).df()

subconsulta_mails_multiples = dd.sql("""
                          SELECT ID_CC, Nombre, Mail
                          FROM centros_culturales
                          WHERE Mail IS NOT NULL
                          AND Mail NOT IN ('s/d', '', '-', '/', '//')
                          AND Mail like '%@%@%'
                          """).df()
                         
mails_cc_multiples = dd.sql("""
                            SELECT ID_CC, Nombre,
                            REPLACE(Mail,' ', ',') AS Mail
                            FROM subconsulta_mails_multiples
                            """).df()
                           
mails_cc_multiples['Mail'] = mails_cc_multiples['Mail'].str.split(',')

def procesar_mails(mails_cc_multiples, cc_mails_simples, indice=737):
    """
    Procesa los mails de 'mails_cc_multiples' y los inserta en 'cc_mails_simples'.
    
    Parámetros:
    - mails_cc_multiples: DataFrame previo a la descomposicón de Mail en valores atómicos.
    - cc_mails_simples: DataFrame donde se guardan los valores procesados.
    - indice: Índice base para la insertar nuevos valores.

    Devuelve:
    - cc_mails_simples actualizado con los valores procesados.
    """
    mails_cc_multiples['Mail'] = mails_cc_multiples['Mail'].apply(lambda x: str(x).split(', ') if isinstance(x, str) else [])

    i = 0
    for index, row in mails_cc_multiples.iterrows():
        for x in row['Mail']:
            cc_mails_simples.loc[indice + i] = [row['ID_CC'], row['Nombre'], x]
            i += 1

    return cc_mails_simples

# Llamamos a la función
cc_mails_simples = procesar_mails(mails_cc_multiples, cc_mails_simples)

# Actualizamos el dataframe final.
mails_cc = dd.sql("""
                    SELECT *
                    FROM cc_mails_simples
                    WHERE Mail NOT IN ( '', ' ');
                  """).df()
                 
mails_cc = dd.sql("""
                  SELECT ID_CC, Nombre, REPLACE(Mail,' ', '') AS Mail
                  FROM mails_cc""").df()


# Eliminamos los dataframe auxiliares
del cc_mails_simples
del subconsulta_mails_multiples
del mails_cc_multiples
#%%

"""----------------------------------Métrica 2 - Establecimientos Educativos----------------------------------------"""
# Asignar la fila 5 como nombres de columna
establecimientos_educativos.columns = establecimientos_educativos.iloc[5]

# Eliminar las primeras 6 filas para comenzar desde la fila 6
establecimientos_educativos = establecimientos_educativos.iloc[6:].reset_index(drop=True)

# El departamento de TOLHUIN estaba mal cargado como dato y estaba en 'Localidad' por error. Lo corregimos
establecimientos_educativos.loc[establecimientos_educativos['Localidad'] == 'TOLHUIN', ['Departamento', 'Código de localidad']] = ['TOLHUIN', 94011010]

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
    SELECT 100 - (COUNT(*) * 100 / (SELECT COUNT(*) FROM establecimientos_educativos)) AS porcentaje_invalidos
    FROM nrosvalidos 
    """
    ).df()


# Renombro Código de localidad por comodidad, le cambiamos también el tipo
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

# Me quedo solo con aquellos EE que tengan al menos un tipo de nivel educativo común
establecimientos_educativos = establecimientos_educativos[~(establecimientos_educativos[cols].sum(axis=1) == 0)]

# Pasamos a mayuscula los nombres de provincia y departamento para estandarizar
establecimientos_educativos["Jurisdicción"] = establecimientos_educativos["Jurisdicción"].str.upper()
establecimientos_educativos["Departamento"] = establecimientos_educativos["Departamento"].str.upper()

# Corregimos el 1§ DE MAYO
establecimientos_educativos.loc[(establecimientos_educativos["Jurisdicción"] == "CHACO") & (establecimientos_educativos["Departamento"] == "1§ DE MAYO"), "Departamento"] = "1 DE MAYO"

# Eliminamos la lista cols auxiliar
del cols
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
            # Obtenemos el índice de la fila del área
            area_index = area_fila.index[0]
            codigo_area = padron_poblacion.loc[area_index, "Unnamed: 1"]
            nombre_area = padron_poblacion.loc[area_index, "Unnamed: 2"]

            # Contamos filas hasta el próximo "AREA #"
            fila_actual = area_index + 2  # Saltamos 2 filas después del área (Todas las tablas arrancan dos filas despues del 'AREA #')
            contador = -3 # Corrijo un desface que me produce la función al final (por eso arranco de -3, pues todas las areas tenían 3 mas que el largo real)

            while fila_actual < total_filas:
                if isinstance(padron_poblacion.loc[fila_actual, "Unnamed: 1"], str) and "AREA #" in padron_poblacion.loc[fila_actual, "Unnamed: 1"]:
                    break  # Paramos de contar si encontramos un área
                contador += 1
                fila_actual += 1

            largo_areas.append((codigo_area, nombre_area, contador))
            i = fila_actual
    
    # Corregimos el último registro manualmente, porque la función continúa hasta el resumen del censo
    largo_areas[len(largo_areas)-1] = tuple(('AREA # 94015', 'Ushuaia', 102))

    return largo_areas

def extraer_bloques_variable_longitudes(
    df,
    areas_info,
    indice_inicial=15, 
    largo_salgo=5
):
    """
    df: DataFrame original.
    areas_info: lista de tuplas (nombre_area, largo), en orden. 
                Ej: [("AREA # 02007", 110), ("AREA # 02014", 108), ...]
    indice_inicial: Es el indice que nos asegura arrancar a contar desde los datos del primer registro censal.
    skip_size: Cuántas filas saltar para cada bloque.

    Devuelve: DataFrame concatenado de todos los bloques, 
              con una columna 'Area' que indica el nombre de área.
    """
    
    df = df.reset_index(drop=True)
    
    indice_actual = indice_inicial
    bloques = []
    
    for codigo_area, nombre_area, largo in areas_info:
        # Verifica si hay suficientes filas para tomar 'largo' desde indice_actual
        if indice_actual + largo > len(df):
            # No hay más filas suficientes, cortamos aquí
            break
        
        # Extraer 'largo' filas
        bloque = df.iloc[indice_actual : indice_actual + largo].copy()
        
        # Asignamos el nombre y el código del área
        bloque['Area'] = codigo_area
        bloque['Descripción'] = nombre_area
        
        bloques.append(bloque)
        
        # Avanzar el puntero length + largo_salgo
        indice_actual += largo + largo_salgo
    
    # Unir todos los bloques en un DataFrame
    if bloques:
        return pd.concat(bloques, ignore_index=True)
    else:
        return pd.DataFrame()


padron_poblacion = extraer_bloques_variable_longitudes(padron_poblacion, calcular_largo_areas())

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

# Pasamos a mayuscula los nombres de Areas para estandarizar
padron_poblacion["Descripción"] = padron_poblacion["Descripción"].str.upper()
#%%
"""-----------------------------------------------------------------------------------------------------------------"""
""" 
                                     ###########################
                                     #####  Normalización  ##### 
                                     ###########################
                                     
    'En las siguientes 3 celdas normalizamos los tres datasets de la manera explicada a detalle en el informe'
"""
#%%
"""---------------------------------------Centros Culturales--------------------------------------------------------"""

# Sacamos los mails de cc para respetar la 1FN

centros_culturales = centros_culturales[['ID_CC', 'ID_DEPTO', 'Nombre', 'Capacidad']].drop_duplicates()

#%%
"""----------------------------------Establecimientos Educativos----------------------------------------------------"""
# Función para extraer el código de provincia a partir del codigo de localidad
def extraer_id_provincia(cod_loc):
    cod_loc = str(cod_loc)  # Convertir a string para evitar errores
    return cod_loc[:2] if len(cod_loc) == 8 else cod_loc[:1]

departamento = establecimientos_educativos.iloc[:, [3, 4]].drop_duplicates()

departamento["ID_PROV"] = departamento["cod_loc"].apply(extraer_id_provincia).astype(int)

departamento["ID_DEPTO"] = departamento["cod_loc"].apply(extraer_id_depto).astype(int)

# Antes de sacar las comunas, guardamos los datos de ellas para la visualización por departamento
comunas_departamentos = departamento.copy()

# Ahora reemplazo "COMUNA X" por "CIUDAD DE BUENOS AIRES"
departamento.loc[departamento['Departamento'].str.startswith('COMUNA'), 'Departamento'] = 'CIUDAD DE BUENOS AIRES'

# Agregamos el id de departamento al dataframe cómo proponemos en nuestro DER
establecimientos_educativos["ID_DEPTO"] = establecimientos_educativos["cod_loc"].apply(extraer_id_depto).astype(int)

# Antes de sacar las comunas, guardamos los datos de ellas para la visualización por establecimientos educativos
comunas_ee = establecimientos_educativos.copy()
comunas_ee["ID_DEPTO"] = comunas_ee["cod_loc"].apply(extraer_id_depto).astype(int)

# Reemplazar los valores entre 2000 y 3000 por 2000 en la columna ID_DEPTO
departamento["ID_DEPTO"] = departamento["ID_DEPTO"].apply(lambda x: 2000 if 2000 <= x <= 3000 else x)
establecimientos_educativos["ID_DEPTO"] = establecimientos_educativos["ID_DEPTO"].apply(lambda x: 2000 if 2000 <= x <= 3000 else x)

# Corregimos los depto con diferencias de ID con la tabla Padrón
departamento["ID_DEPTO"] = departamento["ID_DEPTO"].apply(lambda x: x + 1 if x in [94007, 94014] else x)
establecimientos_educativos["ID_DEPTO"] = establecimientos_educativos["ID_DEPTO"].apply(lambda x: x + 1 if x in [94007, 94014] else x)
comunas_ee["ID_DEPTO"] = comunas_ee["ID_DEPTO"].apply(lambda x: x + 1 if x in [94007, 94014] else x)
comunas_departamentos["ID_DEPTO"] = comunas_departamentos["ID_DEPTO"].apply(lambda x: x + 1 if x in [94007, 94014] else x)

# Eliminamos datos sin uso 
departamento = departamento.iloc[:, [3,2,1]].drop_duplicates()
establecimientos_educativos = establecimientos_educativos.iloc[:, [1, 11, 2, 6, 7, 8, 9, 10]].drop_duplicates()
comunas_departamentos = comunas_departamentos.iloc[:, [3,2,1]].drop_duplicates()

# Nuevo DataFrame con los cueanexos y los niveles educativos de cada EE
niveles_por_ee_expandido = []

for _, row in establecimientos_educativos.iterrows():
    if row["Nivel inicial - Jardín maternal"] == 1 or row["Nivel inicial - Jardín de infantes"] == 1:
        niveles_por_ee_expandido.append([row["Cueanexo"], 0])  # Jardín
    if row["Primario"] == 1:
        niveles_por_ee_expandido.append([row["Cueanexo"], 1])  # Primario
    if row["Secundario"] == 1:
        niveles_por_ee_expandido.append([row["Cueanexo"], 2])  # Secundario
    if row["Secundario - INET"] == 1:
        niveles_por_ee_expandido.append([row["Cueanexo"], 3])  # Secundario INET

# Convertir la lista en un DataFrame
niveles_por_ee = pd.DataFrame(niveles_por_ee_expandido, columns=["Cueanexo", "Nivel_Educativo"])

# Nuevo dataframe con la descripción de cada nivel educativo

niveles_educativos = pd.DataFrame(list({0: 'Jardín', 1: 'Primario', 2: 'Secundario', 3: 'Secundario - INET'}.items()), columns=['Código', 'Nivel'])

# Organizo los datos :)
establecimientos_educativos = establecimientos_educativos.iloc[:,[0,1,2]]
del niveles_por_ee_expandido

#%%
"""---------------------------------------Padrón Población----------------------------------------------------------"""

# Creamos el df auxiliar area_censal 
area_censal = padron_poblacion.iloc[:, [2, 3]].drop_duplicates()

# Antes de reemplazar las comunas, guardamos los datos de ellas para la visualización por departamento (explicado en el info)
comunas_padron = padron_poblacion.copy().rename(columns={'Area': 'ID_DEPTO'})

padron_poblacion = padron_poblacion.iloc[:, [2, 0, 1]].drop_duplicates()

# Función corregida para extraer el código de provincia
def extraer_id_provincia(Area):
    Area = str(Area)  # Convertir a string para evitar errores
    return Area[:2] if len(Area) == 5 else Area[:1]

area_censal["ID_PROV"] = area_censal["Area"].apply(extraer_id_provincia).astype(int)

# Convertir todas las áreas "dosmil y algo" (2000 <= Area < 3000) en 2000
padron_poblacion.loc[padron_poblacion['Area'].between(2000, 2999), 'Area'] = 2000

# Agrupar por Edad y sumar los casos
padron_poblacion = padron_poblacion.groupby(['Area', 'Edad'], as_index=False)['Casos'].sum()

# Reemplzaco "COMUNA X" por "CIUDAD DE BUENOS AIRES"
area_censal.loc[area_censal['Descripción'].str.startswith('COMUNA'), 'Descripción'] = 'CIUDAD DE BUENOS AIRES'

# Convertir todas las áreas 2000 y algo (2000 <= Area < 3000) en 2000
area_censal.loc[area_censal['Area'].between(2000, 2999), 'Area'] = 2000

area_censal = area_censal.drop_duplicates()

# Ahora juntemos el area censal con 'departamento':
area_censal = area_censal.rename(columns={'Area': 'ID_DEPTO', 'Descripción': 'Departamento'})

# Unir ambos DataFrames sin repetir registros
departamento = pd.concat([departamento, area_censal], ignore_index=True).drop_duplicates()

# Borrar el DataFrame `area_censal` para liberar memoria
del area_censal

departamento = departamento.drop_duplicates(subset=['ID_PROV', 'ID_DEPTO'], keep='first')

padron_poblacion = padron_poblacion.rename(columns={'Area': 'ID_DEPTO'})

#%% 
""" 
                                     ###########################
                                     #####  Consultas SQL  ##### 
                                     ###########################
"""
#%%
# i) Jadrín maternal hasta los 2 años (VER LO DE LOS 45 DIAS)
#    Jardín de infantes de 3 a 5 


# LA QUE VA ES RELACIONAR EL CODIGO DE LOCALIDAD CON EL AREA
"""------------------------------------------Ejercicio i)-----------------------------------------------------------"""

cantidad_ee = dd.sql("""
              	SELECT ID_DEPTO,
                      	SUM(CASE WHEN Nivel_Educativo = 0 THEN 1 ELSE 0 END) AS Jardines,
                      	SUM(CASE WHEN Nivel_Educativo = 1 THEN 1 ELSE 0 END) AS Primarios,
                      	SUM(CASE WHEN Nivel_Educativo = 2 OR Nivel_Educativo = 3 THEN 1 ELSE 0 END) AS Secundarios
                    	 
              	FROM niveles_por_ee as n
              	INNER JOIN establecimientos_educativos as e
              	ON e.Cueanexo = n.Cueanexo
              	GROUP BY ID_DEPTO
              	""").df()
             	 
cantidad_alumnos = dd.sql(
        	"""
	SELECT p.ID_DEPTO, d.ID_PROV, d.Departamento,
    	SUM(CASE WHEN Edad BETWEEN 0 AND 5 THEN Casos ELSE 0 END) AS 'poblacion_jardin',
    	SUM(CASE WHEN Edad BETWEEN 6 AND 11 THEN Casos ELSE 0 END) AS 'poblacion_primaria',
    	SUM(CASE WHEN Edad BETWEEN 12 AND 18 THEN Casos ELSE 0 END) AS 'poblacion_secundaria'
   	 
	FROM padron_poblacion AS p
    
	JOIN departamento AS d
	ON p.ID_DEPTO = d.ID_DEPTO
	GROUP BY p.ID_DEPTO, d.ID_PROV, d.Departamento
        	""").df()
                                     	 
ejercicio_i = dd.sql("""
                 	SELECT p.Provincia, d.Departamento,
                 	Jardines, poblacion_jardin AS 'Población Jardín',
                 	Primarios, poblacion_primaria AS 'Población Primaria',
                 	Secundarios, poblacion_secundaria AS 'Población Secundaria'
                	
                    FROM departamento AS d

                 	JOIN provincia AS p
                 	ON p.ID_PROV =  d.ID_PROV
                     
                    LEFT OUTER JOIN cantidad_alumnos AS ca
                    ON d.ID_DEPTO = ca.ID_DEPTO
                	 
                    LEFT JOIN cantidad_ee AS ce
                	ON ce.ID_DEPTO = ca.ID_DEPTO
                	 
                 	ORDER BY p.Provincia ASC, Primarios DESC
                 	""").df()
                     
#%%
"""------------------------------------------Ejercicio ii)----------------------------------------------------------"""

ejercicio_ii = dd.sql("""
                	SELECT Departamento, Provincia, count(*) as Cantidad
                	FROM centros_culturales AS c
               	 
                	JOIN departamento AS d
                	ON C.ID_DEPTO = d.ID_DEPTO
               	 
                	JOIN provincia AS p
                	ON p.ID_PROV = d.ID_PROV
            	WHERE Capacidad > 100
                	GROUP BY Provincia, Departamento
                	ORDER BY Provincia ASC, Cantidad DESC
                	""").df()
                    
#%%
"""------------------------------------------Ejercicio iii)---------------------------------------------------------"""

cantidad_cc_por_deptos = dd.sql("""
              	SELECT ID_DEPTO, count(*) as cc_por_depto
              	FROM centros_culturales
              	GROUP BY ID_DEPTO
              	""").df()
            	 
cantidad_ee_por_deptos = dd.sql("""
              	SELECT ID_DEPTO, count(*) AS ee_por_depto
              	FROM establecimientos_educativos
              	GROUP BY ID_DEPTO
              	""").df()
              	 
total_pob_por_deptos = dd.sql("""
                      	SELECT ID_DEPTO, SUM(Casos) as poblacion
                      	FROM padron_poblacion
                      	GROUP BY ID_DEPTO
                      	""").df()    
                      	 
total_pob_por_depto_con_nombre = dd.sql("""
                                	SELECT Provincia, Departamento, poblacion, d.ID_DEPTO
                                	FROM departamento AS d                         	 
                                	LEFT OUTER JOIN total_pob_por_deptos AS t
                                	ON d.ID_DEPTO = t.ID_DEPTO
                              	 
                                	LEFT OUTER JOIN provincia AS p
                                	ON d.ID_PROV = p.ID_PROV                             	 
                                	""").df()
            	 
ejercicio_iii = dd.sql("""
                            	SELECT t.Departamento, t.Provincia,
                            	CASE WHEN cc_por_depto IS NULL
                                        	THEN 0
                                        	ELSE cc_por_depto
                                    	END AS cc_por_depto,
                                    	ee_por_depto, t.poblacion,
                            	 
                            	FROM total_pob_por_depto_con_nombre AS t
                            	LEFT OUTER JOIN cantidad_ee_por_deptos as e
                            	ON t.ID_DEPTO = e.ID_DEPTO
                            	 
                            	LEFT OUTER JOIN cantidad_cc_por_deptos as c
                            	ON t.ID_DEPTO = c.ID_DEPTO
                            	ORDER BY ee_por_depto DESC, cc_por_depto DESC, t.Provincia ASC, t.Departamento ASC
                            	""").df()

#%%
"""------------------------------------------Ejercicio iv)----------------------------------------------------------"""

depto_provincia = dd.sql("""
                        SELECT d.ID_DEPTO, d.Departamento, p.Provincia
                        FROM departamento AS d
                        
                        JOIN provincia AS p
                        ON p.ID_PROV = d.ID_PROV
                        
                        """).df()
                        
mails_con_depto = dd.sql("""
                        SELECT c.ID_DEPTO, m.Mail
                        FROM mails_cc AS m
                        
                        JOIN centros_culturales AS c
                        ON c.ID_CC = m.ID_CC
                        
                        """).df()
                        
provincia_depto_mail_dominio = dd.sql("""
                        SELECT c1.ID_DEPTO, c1.Provincia, c1.Departamento, c2.Mail, 
                        LOWER(
                            LEFT(
                                SUBSTRING(c2.Mail FROM POSITION('@' IN c2.Mail) + 1),
                                CAST(POSITION('.' IN SUBSTRING(c2.Mail FROM POSITION('@' IN c2.Mail) + 1)) AS INTEGER) - 1
                            )
                        ) AS Dominio
                        FROM depto_provincia AS c1
                        
                        LEFT OUTER JOIN mails_con_depto AS c2  
                        ON c1.ID_DEPTO = c2.ID_DEPTO;
                        """).df()



cantidad_dominios_departamento = dd.sql("""
                                        SELECT ID_DEPTO, Provincia, Departamento, dominio, COUNT(dominio) AS cantidad
                                        FROM provincia_depto_mail_dominio
                                        GROUP BY ID_DEPTO, Provincia, Departamento, dominio
                                        """).df()
                                          
                    
ejercicio_iv = dd.sql("""
                      SELECT ID_DEPTO, c.Provincia, c.Departamento, c.dominio
                      FROM cantidad_dominios_departamento c
                      WHERE c.cantidad = (
                          SELECT MAX(cantidad)
                          FROM cantidad_dominios_departamento
                          WHERE cantidad_dominios_departamento.ID_DEPTO = c.ID_DEPTO)
                      """).df()
#%% 
""" 
                                     ###########################
                                     #####  Visualización  ##### 
                                     ###########################
"""
#%%
"""------------------------------------------Ejercicio i)-----------------------------------------------------------"""

cantidad_de_cc_por_provincia = dd.sql("""
                                  SELECT d.ID_PROV, sum(cc_por_depto) as cant_de_cc_por_prov
                                  FROM cantidad_cc_por_deptos AS cc
                                  
                                  JOIN departamento AS d
                                  ON d.ID_DEPTO = cc.ID_DEPTO
                                  
                                  GROUP BY d.ID_PROV
""").df()


cantidad_de_cc_por_provincia_con_nombre = dd.sql("""
                                  SELECT
                                      CASE
                                          WHEN c.cant_de_cc_por_prov IS NULL
                                          THEN 0
                                          ELSE cant_de_cc_por_prov
                                      END AS cant_de_cc_por_prov,
                                      REPLACE(REPLACE(Provincia, 'TIERRA DEL FUEGO, ANTÁRTIDA E ISLAS DEL ATLÁNTICO SUR', 'TIERRA DEL FUEGO...'), 'CIUDAD AUTÓNOMA DE BUENOS AIRES', 'CABA') AS Provincia
                                  FROM provincia as p
                                  LEFT OUTER JOIN cantidad_de_cc_por_provincia AS c
                                  ON p.ID_PROV = c.ID_PROV
                                  ORDER BY cant_de_cc_por_prov DESC
    """).df()


fig, ax = plt.subplots()
plt.rcParams['font.family'] = 'sans-serif'


ax.bar(data = cantidad_de_cc_por_provincia_con_nombre, x='Provincia', height='cant_de_cc_por_prov')


ax.set_title('Cantidad de centros culturales por provincias')
ax.set_ylabel('Cantidad de centros culturales', fontsize='medium')
plt.tight_layout()
plt.xticks(rotation=-60, fontsize=5, ha = 'left')
ax.bar_label(ax.containers[0], fontsize=6)
ax.set_yticks([])

fig.savefig('i')

#%%
"""------------------------------------------Ejercicio ii)----------------------------------------------------------"""
             

cantidad_ee_comunas = dd.sql("""
    SELECT 
        d.ID_PROV, d.ID_DEPTO, d.Departamento,
        SUM(CASE WHEN "Nivel inicial - Jardín maternal" = 1 OR "Nivel inicial - Jardín de infantes" = 1 THEN 1 ELSE 0 END) AS Jardines,
        SUM(CASE WHEN "Primario" = 1 THEN 1 ELSE 0 END) AS Primarios,
        SUM(CASE WHEN "Secundario" = 1 OR "Secundario - INET" = 1 THEN 1 ELSE 0 END) AS Secundarios
    FROM comunas_ee AS e
    JOIN comunas_departamentos AS d
    ON e.ID_DEPTO = d.ID_DEPTO
    WHERE d.ID_DEPTO > 3000
    GROUP BY d.ID_PROV, d.ID_DEPTO, d.Departamento

    UNION

    SELECT 
        d.ID_PROV, d.ID_DEPTO, d.Departamento,
        SUM(CASE WHEN "Nivel inicial - Jardín maternal" = 1 OR "Nivel inicial - Jardín de infantes" = 1 THEN 1 ELSE 0 END) AS Jardines,
        SUM(CASE WHEN "Primario" = 1 THEN 1 ELSE 0 END) AS Primarios,
        SUM(CASE WHEN "Secundario" = 1 OR "Secundario - INET" = 1 THEN 1 ELSE 0 END) AS Secundarios
    FROM comunas_ee AS e
    JOIN comunas_departamentos AS d
    ON d.Departamento = e.Departamento
    WHERE d.ID_DEPTO < 3000
    GROUP BY d.ID_PROV, d.ID_DEPTO, d.Departamento
""").df()

# El siguiente dataset tiene más datos de los que son usados inmediatamente para el primer gráfico, pero que usamos posteriormente para ver otra relación       

centros_educativos_por_departamento = dd.sql("""
    SELECT 
        p.ID_DEPTO, c.Jardines, c.Primarios, c.Secundarios,
        SUM(p.Casos) AS total_poblacion,
        (Jardines + Primarios + Secundarios) AS total_ee,
        SUM(CASE WHEN p.Edad BETWEEN 0 AND 5 THEN p.Casos ELSE 0 END) AS total_jardin,
        SUM(CASE WHEN p.Edad BETWEEN 6 AND 11 THEN p.Casos ELSE 0 END) AS total_primario,
        SUM(CASE WHEN p.Edad BETWEEN 12 AND 18 THEN p.Casos ELSE 0 END) AS total_secundario
    FROM comunas_padron AS p
    JOIN cantidad_ee_comunas AS c
    ON p.ID_DEPTO = c.ID_DEPTO
    WHERE p.ID_DEPTO > 3000
    GROUP BY p.ID_DEPTO, Jardines, Primarios, Secundarios

    UNION

    SELECT 
        p.ID_DEPTO, c.Jardines, c.Primarios, c.Secundarios,
        SUM(p.Casos) AS total_poblacion,
        (Jardines + Primarios + Secundarios) AS total_ee,
        SUM(CASE WHEN p.Edad BETWEEN 0 AND 5 THEN p.Casos ELSE 0 END) AS total_jardin,
        SUM(CASE WHEN p.Edad BETWEEN 6 AND 11 THEN p.Casos ELSE 0 END) AS total_primario,
        SUM(CASE WHEN p.Edad BETWEEN 12 AND 18 THEN p.Casos ELSE 0 END) AS total_secundario
    FROM comunas_padron AS p
    JOIN cantidad_ee_comunas AS c
    ON p.Descripción = c.Departamento
    WHERE p.ID_DEPTO < 3000
    GROUP BY p.ID_DEPTO, Jardines, Primarios, Secundarios
""").df()

# Orden de colores 1

fig, ax = plt.subplots(figsize=(10, 6))

plt.rcParams['font.family'] = 'sans-serif'

# Definimos una paleta colorblind :)
palette = sns.color_palette("colorblind", n_colors=20)

# Asignamos cada color a una variable
color_jardin     = 'black'       # palette[17]  # naranjita
color_primario   = palette[0]  # azul
color_secundario = palette[11]  # naranja

ax.scatter(
    centros_educativos_por_departamento['total_poblacion'],
    centros_educativos_por_departamento['Jardines'],
    s=6, color=color_jardin, alpha = 0.5, label="Jardín"
)

ax.scatter(
    centros_educativos_por_departamento['total_poblacion'],
    centros_educativos_por_departamento['Primarios'],
    s=6, color=color_primario, alpha = 0.47, label="Primario"
)

ax.scatter(
    centros_educativos_por_departamento['total_poblacion'],
    centros_educativos_por_departamento['Secundarios'],
    s=6, color=color_secundario, alpha = 0.44, label="Secundario"
)

ax.set_xlabel('Cantidad de habitantes', fontsize='medium')  
ax.set_ylabel('Cantidad de Establecimientos Educativos por nivel', fontsize='medium')  
ax.legend(title="Nivel Educativo", loc="upper left")

fig.savefig('ii')
#%% Orden de colores 2

fig, ax = plt.subplots(figsize=(10, 6))

plt.rcParams['font.family'] = 'sans-serif'


ax.scatter(
    centros_educativos_por_departamento['total_poblacion'],
    centros_educativos_por_departamento['Primarios'],
    s=10, color="blue", label="Primario"
)

ax.scatter(
    centros_educativos_por_departamento['total_poblacion'],
    centros_educativos_por_departamento['Secundarios'],
    s=10, color="red", label="Secundario"
)

ax.scatter(
    centros_educativos_por_departamento['total_poblacion'],
    centros_educativos_por_departamento['Jardines'],
    s=10, color="green", label="Jardín"
)


ax.set_xlabel('Cantidad de habitantes', fontsize='medium')  
ax.set_ylabel('Cantidad de Establecimientos Educativos por nivel', fontsize='medium')  
ax.legend(title="Nivel Educativo", loc="upper left")

#%% Orden de colores 3

fig, ax = plt.subplots(figsize=(10, 6))

plt.rcParams['font.family'] = 'sans-serif'


ax.scatter(
    centros_educativos_por_departamento['total_poblacion'],
    centros_educativos_por_departamento['Secundarios'],
    s=10, color="red", label="Secundario"
)

ax.scatter(
    centros_educativos_por_departamento['total_poblacion'],
    centros_educativos_por_departamento['Jardines'],
    s=10, color="green", label="Jardín"
)

ax.scatter(
    centros_educativos_por_departamento['total_poblacion'],
    centros_educativos_por_departamento['Primarios'],
    s=10, color="blue", label="Primario"
)

ax.set_xlabel('Cantidad de habitantes', fontsize='medium')  
ax.set_ylabel('Cantidad de Establecimientos Educativos por nivel', fontsize='medium')  
ax.legend(title="Nivel Educativo", loc="upper left")

#%%

fig, ax = plt.subplots(figsize=(10, 6))


plt.rcParams['font.family'] = 'sans-serif'

ax.scatter(
    centros_educativos_por_departamento['total_poblacion'],
    centros_educativos_por_departamento['total_jardin'],
    s=10, color=color_jardin, label="Jardín"
)

ax.scatter(
    centros_educativos_por_departamento['total_poblacion'],
    centros_educativos_por_departamento['total_primario'],
    s=10, color=color_primario, label="Primario"
)

ax.scatter(
    centros_educativos_por_departamento['total_poblacion'],
    centros_educativos_por_departamento['total_secundario'],
    s=10, color=color_secundario, label="Secundario"
)

ax.set_xlabel('Cantidad de habitantes', fontsize='medium')  
ax.set_ylabel('Cantidad de Alumnos por nivel educativo', fontsize='medium')  
ax.legend(title="Nivel Educativo", loc="upper left")

#%% R2

# Definimos x e y para cada nivel
X = centros_educativos_por_departamento['total_poblacion'].values.reshape(-1, 1)

Y_jardin = centros_educativos_por_departamento['total_jardin'].values
Y_primario = centros_educativos_por_departamento['total_primario'].values
Y_secundario = centros_educativos_por_departamento['total_secundario'].values

# calculamos el r2 de cada nivel
def calcular_r2(X, Y):
    modelo = LinearRegression()
    modelo.fit(X, Y)
    return r2_score(Y, modelo.predict(X))

# Calcular R^2 para cada caso
r2_jardin = calcular_r2(X, Y_jardin)
r2_primario = calcular_r2(X, Y_primario)
r2_secundario = calcular_r2(X, Y_secundario)

# Mostrar resultados
print(f"R2 Jardín: {r2_jardin:.4f}")
print(f"R2 Primario: {r2_primario:.4f}")
print(f"R2 Secundario: {r2_secundario:.4f}")

#%% Gráficos EE y Población por provincias

cantidad_ee_por_provincia = dd.sql(
            """
    SELECT d.ID_PROV, COUNT(*)
    FROM establecimientos_educativos AS e
    
    JOIN departamento AS d
    ON d.ID_DEPTO = e.ID_DEPTO
    
    GROUP BY d.ID_PROV
            """).df()
            
poblacion_por_provincia = dd.sql(
            """
    SELECT d.ID_PROV, SUM(Casos) AS poblacion
    FROM padron_poblacion AS p
    
    JOIN departamento AS d
    ON d.ID_DEPTO = p.ID_DEPTO
    
    GROUP BY d.ID_PROV
            """).df()
            
#%%

poblacion_por_provincia_con_nombre = dd.sql("""
                                  SELECT *
                                  FROM provincia as p
                                  LEFT OUTER JOIN poblacion_por_provincia AS t
                                  ON p.ID_PROV = t.ID_PROV
                                  ORDER BY poblacion DESC                                  
""").df()


poblacion_por_provincia_con_nombre["Provincia"] = poblacion_por_provincia_con_nombre["Provincia"].replace({
    "TIERRA DEL FUEGO, ANTÁRTIDA E ISLAS DEL ATLÁNTICO SUR": "TIERRA DEL FUEGO",
    "CIUDAD AUTÓNOMA DE BUENOS AIRES": "CABA"
})

fig, ax = plt.subplots()


plt.rcParams['font.family'] = 'sans-serif'


ax.bar(data = poblacion_por_provincia_con_nombre, x='Provincia', height='poblacion')


ax.set_title('Poblacion por provincias')
ax.set_xlabel('')
ax.set_ylabel('Poblacion', fontsize='medium')


plt.tight_layout()
plt.xticks(rotation=-60, fontsize=5, ha = 'left')
            
fig.savefig('población por provincia.')

#%%
"""------------------------------------------Visualiación iii)----------------------------------------------------------"""
                 
#Realizar un boxplot por cada provincia, de la cantidad de EE por cada
#departamento de la provincia. Mostrar todos los boxplots en una misma
#figura, ordenados por la mediana de cada provincia

cantidad_ee_por_comunas = dd.sql("""
                  SELECT ID_DEPTO, count(*) AS ee_por_depto
                  FROM comunas_ee
                  GROUP BY ID_DEPTO
                  """).df()

comunas_departamentos_provincias = dd.sql("""
SELECT d.ID_DEPTO, p.Provincia, d.Departamento
FROM comunas_departamentos AS d
JOIN provincia AS p
ON p.ID_PROV = d.ID_PROV
""").df()

ee_por_depto_prov_comunas = dd.sql("""
SELECT cd.ID_DEPTO, REPLACE(REPLACE(Provincia, 'TIERRA DEL FUEGO, ANTÁRTIDA E ISLAS DEL ATLÁNTICO SUR', 'TIERRA DEL FUEGO...'), 'CIUDAD AUTÓNOMA DE BUENOS AIRES', 'CABA') AS Provincia, ee_por_depto
FROM comunas_departamentos_provincias AS cd
LEFT OUTER JOIN cantidad_ee_por_comunas AS ce
ON cd.ID_DEPTO = ce.ID_DEPTO
""").df()

fig, ax = plt.subplots(figsize=(10, 6))

sns.boxplot(x='Provincia',
            y='ee_por_depto',
            data=ee_por_depto_prov_comunas,
            ax=ax,
            showmeans=True,
            order = ee_por_depto_prov_comunas.groupby('Provincia')['ee_por_depto'].median().sort_values().index)

ax.set_xticklabels(ax.get_xticklabels(), rotation=-60, fontsize=8, ha='left')

ax.set_ylabel('Cantidad de Establecimientos Educativos por Departamento', fontsize=10)
ax.set_title(' ')
plt.tight_layout()
plt.subplots_adjust(top=0.9)  

fig.savefig('iii')
#%%            
"""------------------------------------------Ejercicio iv)-----------------------------------------------------------"""           

proporciones = ejercicio_iii.copy()

# Agregamos las proporciones correctas al DataFrame
proporciones["proporcion_ee_1000_hab"] = (proporciones["ee_por_depto"] / proporciones["poblacion"]) * 1000
proporciones["proporcion_cc_1000_hab"] = (proporciones["cc_por_depto"] / proporciones["poblacion"]) * 1000

# Reemplazamos NaN por 0 en caso de divisiones por 0
ejercicio_iii.fillna(0, inplace=True)

proporciones = proporciones.sort_values("poblacion")


ejercicio_iii_por_prov = dd.sql("""
                                    SELECT Provincia, sum(ee_por_depto) as ee_por_prov, sum(cc_por_depto) as cc_por_prov, sum(poblacion) as poblacion
                                    FROM ejercicio_iii
                                    GROUP BY Provincia
""").df()

proporciones = proporciones.sort_values('poblacion')

proporciones["Provincia"] = proporciones["Provincia"].str.title()  

proporciones["Provincia"] = proporciones["Provincia"].replace({
    "Tierra Del Fuego, Antártida E Islas Del Atlántico Sur": "Tierra del Fuego",
    "Ciudad Autónoma De Buenos Aires": "Caba"
})

df_resultado = proporciones.melt(id_vars=["Provincia"],
                              value_vars=["proporcion_ee_1000_hab", "proporcion_cc_1000_hab"],
                              var_name="Tipo",
                              value_name="Proporción")

plt.figure(figsize=(12, 6))
sns.barplot(data=df_resultado, x="Provincia", y="Proporción", hue="Tipo", palette=["blue", "orange"], errorbar=None)

plt.xticks(rotation=90)

plt.title("Proporción de EE y CC cada 1000 habitantes por provincia")
plt.xlabel("Provincia")
plt.ylabel("Proporción por 1000 habitantes")

plt.xticks(rotation=-60, fontsize=8)  
plt.yticks(fontsize=8)
plt.tight_layout() 

plt.legend(title="Tipo de Establecimiento", labels=["EE cada 1000 hab", "CC cada 1000 hab"])

plt.show()

fig.savefig('iv')
#%% Con factor escalante

# Factor de escala
factor_escala_cc = 30  

proporciones["Provincia"] = proporciones["Provincia"].str.title()  

proporciones["Provincia"] = proporciones["Provincia"].replace({
    "Tierra Del Fuego, Antártida E Islas Del Atlántico Sur": "Tierra del Fuego",
    "Ciudad Autónoma De Buenos Aires": "Caba"
})

df_resultado = proporciones.melt(id_vars=["Provincia"],
                              value_vars=["proporcion_ee_1000_hab", "proporcion_cc_1000_hab"],
                              var_name="Tipo",
                              value_name="Proporción")

df_resultado.loc[df_resultado["Tipo"] == "proporcion_cc_1000_hab", "Proporción"] *= factor_escala_cc

plt.figure(figsize=(12, 6))
sns.barplot(data=df_resultado, x="Provincia", y="Proporción", hue="Tipo", palette=["blue", "orange"], errorbar=None)

plt.xticks(rotation=-50, fontsize=8)  
plt.yticks(fontsize=8)
plt.tight_layout()  

plt.title(" ")
plt.xlabel("Provincia")
plt.ylabel(" ")

plt.legend(title="Tipo de Establecimiento", labels=["EE cada 1000 hab", f"CC cada 30000 hab"])

fig.savefig('iv con factor escala 20 para CC')

#%%
""" 
                                         ##############################
                                         #####  Muchas gracias :) ##### 
                                         ##############################
"""
#%%

"""
Scatterplot simple
"""

# Filtramos los valores donde ambos sean mayores a 0
proporciones_filtradas = proporciones[
    (proporciones["proporcion_cc_1000_hab"] > 0) & (proporciones["proporcion_ee_1000_hab"] > 0)
]

# Extraemos las variables
x = proporciones_filtradas["proporcion_cc_1000_hab"]
y = proporciones_filtradas["proporcion_ee_1000_hab"]
poblacion = proporciones_filtradas["poblacion"]

# Creamos la figura
plt.figure(figsize=(10, 6))

# Scatter plot con tamaño basado en la población
plt.scatter(x, y, alpha=0.9, color="royalblue", edgecolors="black", linewidth=0.5)

# Ajuste estético
plt.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()

# Etiquetas y título
plt.xlabel("Cantidad de CC cada 1000 habitantes", fontsize=12, labelpad=10)
plt.ylabel("Cantidad de EE cada 1000 habitantes", fontsize=12, labelpad=10)

# Guardamos la figura
plt.savefig('iv_relacion_cc_ee_sin_ceros_poblacion.png')

# Mostramos el gráfico
plt.show()

#%%

"""
Veamos si separando por c/habitantes notamos algo más
"""

# Filtramos los valores donde ambos sean mayores a 0
proporciones_filtradas = proporciones[
    (proporciones["proporcion_cc_1000_hab"] > 0) & (proporciones["proporcion_ee_1000_hab"] > 0)
]

# Extraemos las variables
x = proporciones_filtradas["proporcion_cc_1000_hab"]
y = proporciones_filtradas["proporcion_ee_1000_hab"]
poblacion = proporciones_filtradas["poblacion"]

# Definimos los cuartiles de la población para separar los grupos
q1, q2, q3 = np.percentile(poblacion, [25, 50, 75])

# Creamos el layout 2x2
fig, axs = plt.subplots(2, 2, figsize=(12, 10), sharex=True, sharey=True)

# Definimos colores para cada grupo
colores = ["lightblue", "dodgerblue", "royalblue", "midnightblue"]
titulos = ["Baja población", "Media-baja población", "Media-alta población", "Alta población"]
rangos = [
    (poblacion <= q1),
    ((poblacion > q1) & (poblacion <= q2)),
    ((poblacion > q2) & (poblacion <= q3)),
    (poblacion > q3)
]

# Iteramos sobre los subgráficos
for ax, rango, color, titulo in zip(axs.flat, rangos, colores, titulos):
    ax.scatter(x[rango], y[rango], color=color, alpha=0.6)
    ax.set_title(titulo, fontsize=12)
    ax.grid(True, linestyle="--", alpha=0.5)

# Etiquetas en los ejes
for ax in axs[:, 0]:
    ax.set_ylabel("EE cada 1000 hab", fontsize=12)

for ax in axs[1, :]:
    ax.set_xlabel("CC cada 1000 hab", fontsize=12)

# Ajuste de layout
plt.tight_layout()

# Guardamos la figura
plt.savefig('iv_relacion_cc_ee_separado_por_poblacion.png')

#%%

# Filtramos los valores donde ambos sean mayores a 0
proporciones_filtradas = proporciones[
    (proporciones["proporcion_cc_1000_hab"] > 0) & (proporciones["proporcion_ee_1000_hab"] > 0)
]

# Extraemos las variables
x = proporciones_filtradas["proporcion_cc_1000_hab"]
y = proporciones_filtradas["proporcion_ee_1000_hab"]
poblacion = proporciones_filtradas["poblacion"]

# Definimos los cuartiles de la población para separar los grupos
q1, q2, q3 = np.percentile(poblacion, [25, 50, 75])

# Definimos los rangos de población
rangos = {
    "Baja población": poblacion <= q1,
    "Media-baja población": (poblacion > q1) & (poblacion <= q2),
    "Media-alta población": (poblacion > q2) & (poblacion <= q3),
    "Alta población": poblacion > q3
}

# Creamos una figura con 2 filas y 2 columnas
fig, axs = plt.subplots(2, 2, figsize=(12, 10))

# Color único para todos los gráficos
color = "midnightblue"

# Iteramos sobre cada subgráfico y rango de población
for ax, (titulo, filtro) in zip(axs.flat, rangos.items()):
    x_filtro = x[filtro]
    y_filtro = y[filtro]

    # Scatter plot en la subfigura correspondiente
    ax.scatter(x_filtro, y_filtro, color=color, alpha=0.6)

    # Ajustamos los límites de los ejes individualmente
    ax.set_xlim(0, x_filtro.max() * 1.1 if not x_filtro.empty else 0.01)
    ax.set_ylim(0, y_filtro.max() * 1.1 if not y_filtro.empty else 0.01)

    # Etiquetas y título
    ax.set_xlabel("CC cada 1000 hab", fontsize=10)
    ax.set_ylabel("EE cada 1000 hab", fontsize=10)
    ax.set_title(titulo, fontsize=12)

    # Grid para mejor lectura
    ax.grid(True, linestyle="--", alpha=0.6)

# Ajustamos el diseño para que no se solapen los gráficos
plt.tight_layout()

# Guardamos la figura con los 4 gráficos juntos
plt.savefig('iv_relacion_cc_ee_4juntos.png')

# Mostramos el gráfico con los 4 plots juntos
plt.show()
