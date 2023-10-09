# Databricks notebook source
# MAGIC %run /Shared/LACrime/Authorization.py

# COMMAND ----------

import pandas as pd 
from azure.storage.blob import BlobServiceClient
import numpy as np

file_location = "abfss://container4source@source4la.dfs.core.windows.net/crime.csv"
file_type = "csv"
print(file_location)

# COMMAND ----------


# Read the CSV file into a Spark DataFrame
crimeData = spark.read.csv(file_location, header=True, inferSchema=True)

# 'crimeData' Spark DataFrame
crimeData.show()


# COMMAND ----------

# Count the total number of rows in the DataFrame
total_crimes = crimeData.count()
print("Total number of crimes in the dataset: {}".format(total_crimes))

# Show the first few rows of the DataFrame
crimeData.show()


# COMMAND ----------

from pyspark.sql.functions import count, col
#COUNT the number of crime occurred
crimeByType = crimeData.groupBy('CrmCdDesc').agg(count("*").alias("count"))

# Show the result
crimeByType.show()


# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt

# Extract the 'year' from the 'Date.Rptd' column and create a new column
crimeData = crimeData.withColumn("year", F.substring(crimeData["DateRptd"], 0, 4))

# Group the data by 'year' and count the occurrences
crimeByYear = crimeData.groupBy("year").count().orderBy("year")

# Convert the result to a Pandas DataFrame for plotting
crimeByYear_pd = crimeByYear.toPandas()

# Plot the data
crimeByYear_pd.plot(kind='line', x='year', y='count', title='Nombre de crimes par année')
plt.xlabel('Année')
plt.ylabel('Nombre')
plt.show()



# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt

# Boucler à travers chaque année dans crimeByYear
for row in crimeByYear.collect():
    year = row['year']
    
    # Filtrer les données pour l'année donnée
    crimeYear = crimeData.filter(crimeData['year'] == year)
    
    # Compter les crimes les plus courants
    top_crimes = crimeYear.groupBy("CrmCdDesc").count().orderBy(F.desc("count")).limit(10)
    
    # Convertir les résultats en Pandas DataFrame pour le traçage
    top_crimes_pd = top_crimes.toPandas()
    
    # Tracer un graphique à barres pour les 10 crimes les plus courants
    top_crimes_pd.plot(kind='bar', x='CrmCdDesc', y='count', title="Crimes en " + str(year))
    plt.xlabel('Type de crime')
    plt.ylabel('Nombre de crimes')
    plt.show()


# COMMAND ----------

# Utilisez groupBy pour regrouper par 'AREA.NAME' et comptez les occurrences
crimeCountsByArea = crimeData.groupBy('AREANAME').agg(F.count('*').alias('count'))

# Tri des résultats par ordre décroissant de comptage
crimeCountsByArea = crimeCountsByArea.orderBy(F.desc('count'))

# Afficher le résultat
crimeCountsByArea.show()


# COMMAND ----------

# Extraire la colonne 'DATE.OCC' et effectuer le comptage
crimeCountsByOccurrenceDate = crimeData.groupBy('DATEOCC').agg(F.count('*').alias('count')).orderBy('DATEOCC')

# Convertir les résultats en Pandas DataFrame
crimeCountsByOccurrenceDate_pd = crimeCountsByOccurrenceDate.toPandas()

# Traçage du nombre de crimes par date d'occurrence
plt.figure(figsize=(10, 8))
plt.plot(crimeCountsByOccurrenceDate_pd['DATEOCC'], crimeCountsByOccurrenceDate_pd['count'])
plt.title('Crimes survenus')
plt.xlabel('Date')
plt.ylabel('Nombre de crimes')
plt.xticks(rotation=45)
plt.show()

# Extraire la colonne 'Date.Rptd' et effectuer le comptage
crimeCountsByReportedDate = crimeData.groupBy('DateRptd').agg(F.count('*').alias('count')).orderBy('DateRptd')

# Convertir les résultats en Pandas DataFrame
crimeCountsByReportedDate_pd = crimeCountsByReportedDate.toPandas()

# Traçage du nombre de crimes par date de signalement
plt.figure(figsize=(10, 8))
plt.plot(crimeCountsByReportedDate_pd['DateRptd'], crimeCountsByReportedDate_pd['count'], color='r')
plt.title('Crimes signalés')
plt.xlabel('Date')
plt.ylabel('Nombre de crimes')
plt.xticks(rotation=45)
plt.show()


# COMMAND ----------

# Extraire la colonne 'Status.Desc' et effectuer le comptage
crimeStatusCounts = crimeData.groupBy('StatusDesc').agg(F.count('*').alias('count'))

# Convertir les résultats en Pandas DataFrame
crimeStatusCounts_pd = crimeStatusCounts.toPandas()

# Traçage du diagramme circulaire (pie chart) du statut des crimes
plt.figure(figsize=(10, 10))
plt.pie(crimeStatusCounts_pd['count'], labels=crimeStatusCounts_pd['StatusDesc'], autopct='%.2f%%')
plt.title('Statut des crimes')
plt.show()

