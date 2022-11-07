# Databricks notebook source
#variables para conectar al storage/datalake    
bstorage='mystorage1342'
containerbs='input'
accesskeybs='NMlrgoLr5QzzE9FrQ0ooV0tpHJ6NUHmSZvDM+XbcT5r5lEDT50CzzQ38fll0mnben0iAFt8i2SuT+AStEexswA=='

datalake='mydatalake1342'
containerdl='output'
accesskeydl='Vgxcn811yBgeEe/LwFCTmNCg439i0RXl5BR9bYDfXNiCOffmeDe5qxZ3ETjOpPY72lkfpOgGT2be+ASthB5zWQ=='



# COMMAND ----------

import pyspark
import pyspark.sql.functions
from pyspark.sql.functions import countDistinct,col,mean,sum
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Transformaciones-grupo 4").getOrCreate()

# COMMAND ----------

#esta funcion monta el container del  blobstorage/datalake en el databricks en una nueva carpeta 
def montar_almacenamiento(storage,container,accesskey):
    #se crean carpetas sobre las cuales se montan los container
    dbutils.fs.mkdirs('dbfs:/mnt/'+container)
    #se montan los container en las carpetas
    dbutils.fs.mount(
    source = 'wasbs://'+container+'@'+storage+'.blob.core.windows.net' ,
    mount_point = '/mnt/'+container,
    extra_configs = {'fs.azure.account.key.'+storage+'.blob.core.windows.net':accesskey})

#aca guarda la variable transformacion dentro de la carpeta container en el archivo nombretransformacion
def transformacion_almacenar(transformacion,container,nombretransformacion):
    #se guarda la transformacion en la carpeta temporal
    transformacion.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save("dbfs:/mnt/"+container+"/temp")
    #se guarda solo el archivo que tiene las transformaciones en si
    files = dbutils.fs.ls("/mnt/"+container+"/temp")
    output_file = [x for x in files if x.name.startswith("part-")]
    filename=output_file[0].name
    dbutils.fs.mv("dbfs:/mnt/"+container+"/temp/"+filename,"dbfs:/mnt/"+container+"/"+nombretransformacion)
    #se borra archivos temporales
    tempfiles = dbutils.fs.ls("dbfs:/mnt/"+container+"/temp/")
    for x in tempfiles:
        dbutils.fs.rm("dbfs:/mnt/"+container+"/temp/"+x.name,recurse=True)
    

#se monta blobstorage
montar_almacenamiento(bstorage,containerbs,accesskeybs)
#se monta datalake
montar_almacenamiento(datalake,containerdl,accesskeydl)

#se crea carpeta temporal
dbutils.fs.mkdirs('/mnt/'+containerdl+'/temp')

# COMMAND ----------

# MAGIC %md
# MAGIC Transformacion 1

# COMMAND ----------

source=spark.read.csv('dbfs:/mnt/'+containerbs+'/Categoria.csv',header=True,inferSchema=True)
#source.display()
transformacion=source.withColumnRenamed("Categoria", "Nombre_Categoria")
#transformacion.display()
transformacion_almacenar(transformacion,containerdl,"transformacion1.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Transformacion 2

# COMMAND ----------

source=spark.read.csv('dbfs:/mnt/'+containerbs+'/FactMine.csv',header=True,inferSchema=True)
#source.display()
transformacion=source.agg({'TotalOreMined': 'sum'})
#transformacion.display()
transformacion_almacenar(transformacion,containerdl,"transformacion2.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Transformacion 3

# COMMAND ----------

source=spark.read.csv('dbfs:/mnt/'+containerbs+'/Mine.csv',header=True,inferSchema=True)
#source.display()
transformacion=source.select('Country','FirstName','LastName','Age')
#transformacion.display()
transformacion_almacenar(transformacion,containerdl,"transformacion3.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Transformacion 4

# COMMAND ----------

source=spark.read.csv('dbfs:/mnt/'+containerbs+'/Mine.csv',header=True,inferSchema=True)
#source.display()
transformacion=source.groupBy("Country").sum("TotalWasted")
#transformacion.display()
transformacion_almacenar(transformacion,containerdl,"transformacion4.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Transformacion 5

# COMMAND ----------

source=spark.read.csv('dbfs:/mnt/'+containerbs+'/Producto.csv',header=True,inferSchema=True)
#source.display()
transformacion=source.select(countDistinct('Cod_Producto')).withColumnRenamed('count(DISTINCT Cod_Producto)','cantidad_de_productos')
#transformacion.display()
transformacion_almacenar(transformacion,containerdl,"transformacion5.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Transformacion 6

# COMMAND ----------

source=spark.read.csv('dbfs:/mnt/'+containerbs+'/Producto.csv',header=True,inferSchema=True)
#source.display()
transformacion=source.sort(col("Cod_Producto").desc())
#transformacion.display()
transformacion_almacenar(transformacion,containerdl,"transformacion6.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Transformacion 7

# COMMAND ----------

source=spark.read.csv('dbfs:/mnt/'+containerbs+'/SubCategoria.csv',header=True,inferSchema=True)
#source.display()
transformacion=source.filter(source.Cod_SubCategoria  == 3)
#transformacion.display()
transformacion_almacenar(transformacion,containerdl,"transformacion7.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Transformacion 8

# COMMAND ----------

source=spark.read.csv('dbfs:/mnt/'+containerbs+'/VentasInternet.csv',header=True,inferSchema=True)
transformacion=source.withColumn("Ingresos Netos", source.Cantidad*source.PrecioUnitario-source.CostoUnitario)
transformacion_almacenar(transformacion,containerdl,"transformacion8.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Transformacion 9

# COMMAND ----------

transformacion9=transformacion.groupBy('Cod_Producto').agg(mean('Ingresos Netos'),sum('Ingresos Netos')
).orderBy('Cod_Producto',ascedening = True).withColumnRenamed('avg(Ingresos Netos)','promedios(Ingresos Netos)').withColumnRenamed('sum(Ingresos Netos)','Suma_ingresos_netos')
transformacion_almacenar(transformacion9,containerdl,"transformacion9.csv")

# COMMAND ----------

#se borra las carpetas temporales que no se van a usar mas
dbutils.fs.rm("dbfs:/mnt/"+containerdl+"/temp/",recurse=True)
dbutils.fs.rm("dbfs:/mnt/"+containerdl+" _$azuretmpfolder$",recurse=True)
#es necesario desmontar los puntos de montaje despues de ser usados
dbutils.fs.unmount('/mnt/'+containerbs)
dbutils.fs.unmount('/mnt/'+containerdl)
