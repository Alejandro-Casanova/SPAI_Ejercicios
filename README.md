# Ejercicios de la asignatura SPAI (Sistemas y Protocolos de Aplicaciones en Internet)

Estas son mis soluciones para los ejercicios del laboratorio de la asignatura SPAI, del máster en Software de Sistemas Distribuidos y Empotrados. Se utilizaron las herramientas Apache Spark y Hadoop para análisis de datos y machine learning.

### 1. Cuenta-palabras con Java y Hadoop 

Cálculo de la palabra que aparece mas veces en el fichero quijote.txt con un programa MapReduce Hadoop escrito en Java.

Se utilizará como entrada del programa, la salida del programa que usa como entrada el fichero quijote.txt y genera un fichero de salida de tipo texto con una línea por palabra encontrada. Cada línea contiene la palabra y el número de veces que ha aparecido la palabra en el fichero quijote.txt

### 2. Cuenta-palabras con Python y Spark
El objetivo es calcular la palabra que aparece con mayor frecuencia en un fichero de texto (quijote.txt) utilizando como paradigma de programacion los RDDs sobre la plataforma de computacion distribuida Spark y usando como lenguaje de programacion Python.

La paralelizacion de este código se debe realizar utilizando exclusivamente funciones básicas map/reduce (textFile, reduce, reduceByKey, map, flatmap, filter, count).

### 3. Cálculo de medias y varianzas con Spark

Calcular mediante paralelizacion con Spark la media y varianza del dataset data_ok.csv utilizando exclusivamente funciones basicas map/reduce (textFile, reduce, reduceByKey, map, flatmap, filter, count).

Calcular inicialmente para una sola columna y mas tarde para todas las columnas del dataset. Verificar que la solucion propuesta es correcta con las funciones rdd.mean() y rdd.stdev().

* v1: Calcula la media y varianza para la columna 4
* v2: utilizando las operaciones de vectorizacion de python y arrays de numpy, utilizar la misma estructura de codigo de la version 1 para calcular las medias y varianzas de todas las columnas
* v3: Transforma cada celda del dataset en un elemento (j,v),  donde "j" es la columna de la celda y "v" es el valor de la celda del rdd. Resuelve el problema con esta nueva estructura del dataset

### 4. Implementación centralizada de un algoritmo de ML supervisado

Utilizando el conjunto de datos etiquetados "botnet_tot_syn_l.csv", implemente un clasificador de regresión logística con Python.