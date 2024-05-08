#Importacion de librerias
import pandas as pd
import numpy as np
import os

#Importacion de datasets en dataframes
df_orders = pd.read_csv("https://raw.githubusercontent.com/GastonAeschlimann/TP-Alkemy/main/ecommerce_orders_dataset.csv")
df_orders.head()

df_order_items = pd.read_csv("https://raw.githubusercontent.com/GastonAeschlimann/TP-Alkemy/main/ecommerce_order_items_dataset.csv")
df_order_items.head()

df_products = pd.read_csv("https://raw.githubusercontent.com/GastonAeschlimann/TP-Alkemy/main/ecommerce_products_dataset.csv")
df_products.head()

df_customers = pd.read_csv("https://raw.githubusercontent.com/GastonAeschlimann/TP-Alkemy/main/ecommerce_customers_dataset.csv")
df_customers.head()

df_order_payments = pd.read_csv("https://raw.githubusercontent.com/GastonAeschlimann/TP-Alkemy/main/ecommerce_order_payments_dataset.csv")
df_order_payments.head()

#Funcion para obtener informacion acerca de los dataframes
def datosDataFrames(df):
  print("----------------------")
  print("Tamaño DataFrame")
  print(df.shape) #Tamaño del dataframe
  print("----------------------")
  print("----------------------")
  print("Análisis tipo de datos")
  print(df.info()) #Tipos de datos
  print("----------------------")
  print("----------------------")
  print("Recuento de nulos")
  print(df.isnull().sum()) #Analisis de nulos
  print("----------------------")
  print("----------------------")
  print("Recuento de duplicados")
  print(df.duplicated().sum()) #Analisis duplicados

#Aplicacion de funciones
datosDataFrames(df_orders)
datosDataFrames(df_order_items)
datosDataFrames(df_products)
datosDataFrames(df_customers)
datosDataFrames(df_order_payments)

#Definicion de la funcion transformarFechas
def transformarFechas(df, columnas_fechas): #Los parametros son el dataframe y la lista con las columnas a transformar
    df[columnas_fechas] = df[columnas_fechas].apply(pd.to_datetime) #Transforma las columnas de columnas_fechas a datetime
    return df

#Definicion de la funcion transformarString
def transformarString(df, columnas_string): #Los parametros son el dataframe y la lista con las columnas a transformar
    for columna in columnas_string: #Transforma las columnas en columnas_string a string
        df[columna] = df[columna].astype("string")
    return df

#Definicion de la funcion convertirMayusculas
def convertirMayusculas(df, columna): #Los parametros son el dataframe y la lista con las columna a transformar
    df[columna] = df[columna].str.upper() # Convertir todos los valores de la columna a mayusculas
    return df

#Definicion de la funcion convertirMinusculas
def convertirMinusculas(df, columna): #Los parametros son el dataframe y la lista con las columna a transformar
    df[columna] = df[columna].str.lower() # Convertir todos los valores de la columna a minúsculas
    return df

######Trabajo con DataFrame ORDERS#####

# Porcentaje de nulos en cada columna
null_percentages_orders = (df_orders.isnull().sum() / len(df_orders)) * 100

#Veamos cual es el % de nulos
print(null_percentages_orders)

# Filtrado de las columnas con menos del 10% de valores nulos
columns_to_keep_orders = null_percentages_orders[null_percentages_orders <= 10].index

# Eliminacion de las filas con nulos solo en las columnas seleccionadas
df_orders.dropna(subset=columns_to_keep_orders, inplace=True)

#Chequeo cantidad de valores null
df_orders.isnull().sum()

# Lista de columnas que contienen fechas
columnas_fechas_orders = ['order_purchase_timestamp', 'order_approved_at',
                          'order_delivered_carrier_date', 'order_delivered_customer_date',
                          'order_estimated_delivery_date']

#Aplicamos nuestra funcion conversora de fechas
df_orders = transformarFechas(df_orders, columnas_fechas_orders)

# Lista de columnas a transfomar en string
columnas_string_orders = ["order_id","customer_id","order_status"]

#Aplicamos nuestra funcion conversora de string
df_orders = transformarString(df_orders, columnas_string_orders)

#Chequeo de los tipos de datos 
df_orders.info()

#Aplicamos la conversion a minusculas
df_orders = convertirMinusculas(df_orders, "order_status")

######Trabajo con DataFrame ORDER_ITEMS#####

# Lista de columnas que contienen fechas
columnas_fechas_order_items = ['shipping_limit_date']

#Aplicamos nuestra funcion conversora de fechas
df_order_items = transformarFechas(df_order_items, columnas_fechas_order_items)

# Lista de columnas a transfomar en string
columnas_string_order_items = ["order_id","order_item_id","product_id","seller_id"]

#Aplicamos nuestra funcion conversora de string
df_order_items = transformarString(df_order_items, columnas_string_order_items)

######Trabajo con DataFrame PRODUCTS#####

# Porcentaje de nulos en cada columna
null_percentages_products = (df_products.isnull().sum() / len(df_products)) * 100

print(null_percentages_products)

# Filtramos las columnas con menos del 10% de valores nulos
columns_to_keep_products = null_percentages_products[null_percentages_products <= 10].index

# Eliminamos las filas con nulos solo en las columnas seleccionadas
df_products.dropna(subset=columns_to_keep_products, inplace=True)

#Chequeo cantidad de null
df_products.isnull().sum()

# Lista de columnas a transfomar en string
columnas_string_products = ["product_id","product_category_name"]

#Aplicamos nuestra funcion conversora de string
df_products = transformarString(df_products, columnas_string_products)

#Revisamos que los tipos de datos sean correctos
df_products.info()

#Aplicamos la conversion a minusculas
df_products = convertirMinusculas(df_products, "product_category_name")

######Trabajo con DataFrame COSTUMERS#####

# Lista de columnas a transfomar en string
columnas_string_customers = ["customer_id","customer_unique_id","customer_zip_code_prefix","customer_city","customer_state"]

#Aplicamos funcion conversora de string
df_customers = transformarString(df_customers, columnas_string_customers)

#Revisamos que los tipos de datos sean correctos
df_customers.info()

#Aplicamos la conversion a minusculas
df_customers = convertirMinusculas(df_customers, "customer_city")

#Aplicamos la conversion a mayusculas
df_customers = convertirMayusculas(df_customers, "customer_state")

#####Trabajo con DataFrame PAYMENTS#####

# Lista de columnas a transfomar en string
columnas_string_order_payments = ["order_id","payment_type"]

#Aplicamos nuestra funcion conversora de string
df_order_payments = transformarString(df_order_payments, columnas_string_order_payments)

#Aplicamos la conversion a minusculas
df_order_payments = convertirMinusculas(df_order_payments, "payment_type")

#####Analisis DATAFRAMES#####

#Definicion de la función para obtener insights relevantes de los dataframes
def analisisDataFrames(df):
  print("---------------------------------")
  print("Análisis variables cuantitativas")
  columnas_numericas = df.select_dtypes(include=['int', 'float']).columns 
  if not columnas_numericas.empty: 
    print(df[columnas_numericas].describe()) 
  print("---------------------------------")
  print("---------------------------------")
  print("Análisis variables cualitativas")
  columnas_string = df.select_dtypes(include=['string']).columns 
  if not columnas_string.empty: 
    print(df[columnas_string].describe())

#####Analisis DataFrame ORDERS#####

analisisDataFrames(df_orders)

#Tipos de pagos
df_orders["order_status"].value_counts()

#####Analisis DataFrame ORDER ITEMS#####

analisisDataFrames(df_order_items)

#####Analisis DataFrame PRODUCTS#####

analisisDataFrames(df_order_items)

#Analisis de categorias
df_products["product_category_name"].value_counts()

#####Analisis DataFrame CUSTOMERS#####

#Analisis por regiones de los compradores
df_customers["customer_state"].value_counts()

#####Analisis Data Frame PAYMENTS#####

analisisDataFrames(df_order_payments)

#Tipos de pagos
df_order_payments["payment_type"].value_counts()

#Se realiza un merge entre DataFrames ORDER-ORDER PAYMENTS
df_merge_orders_payments = pd.merge(df_orders, df_order_payments, how = "inner", on= "order_id")
df_merge_orders_payments.head()

#Simplificar la informacion quitando las columnas no utilizadas en el analisis visual
df_merge_orders_payments.drop(['customer_id','order_approved_at','order_delivered_carrier_date','order_delivered_customer_date','order_estimated_delivery_date'], axis=1, inplace=True) #Indicamos el nombre de las columnas y que vamos a mantener los cambios
df_merge_orders_payments.head() #Visualizacion del resultado

#Exportar como csv nuevo DataFrame
df_merge_orders_payments.to_csv("C:/Users/pada/Desktop/CLASES/ALKEMY/NUEVOS DATASETS/order_order_payments.csv")

#Se realiza un merge entre DataFrames ORDER ITEMS-PRODUCTS
df_merge_order_items_products = pd.merge(df_order_items, df_products, how = "inner", on= "product_id")
df_merge_order_items_products.head()

#Simplificar la información quitando aquellas columnas que no usaremos en el analisis visual
df_merge_order_items_products.drop(['product_id','seller_id','shipping_limit_date'], axis=1, inplace=True)
df_merge_order_items_products.head() #Visualizacion del resultado

df_merge_order_items_products.to_csv("C:/Users/pada/Desktop/CLASES/ALKEMY/NUEVOS DATASETS/order_items_products.csv")
