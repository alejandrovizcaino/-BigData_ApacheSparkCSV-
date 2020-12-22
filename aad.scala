// AAD Asignatura Big Data: Práctica 1 utilizando Apache Spark
// Autor: Alejandro Vizcaíno Castilla

val csv = spark.read.option("header",true).option("delimiter",";")
   .csv("RIA_exportacion_datos_diarios_Huelva_20140206.csv")

val csv_filtrado = csv.select("IDESTACION", "SESTACION", "FECHA", "RADIACION", "PRECIPITACION")

// Apartado a: Radiación Media

val id_filtro = csv_filtrado.filter(('IDESTACION===2 || 'IDESTACION===3 || 'IDESTACION===4 || 'IDESTACION===4 || 'IDESTACION===5 || 'IDESTACION===6 || 'IDESTACION===7 || 'IDESTACION===8 || 'IDESTACION===9 || 'IDESTACION===10) && (('RADIACION!=="null") || ('PRECIPITACION!=="null")))


val replace_filtro = id_filtro.select('SESTACION, translate('RADIACION, ",", ".").as("RADIACION") )

val agrup = replace_filtro.groupBy("SESTACION")

val media = agrup.agg(avg("RADIACION").as("RADIACION_MEDIA")).sort(desc("RADIACION_MEDIA"))

media.show()

// Apartado b: Precipitaciones totales

val primero = media.select('SESTACION).first.getString(0)
println(primero)

val id_filtro2 = id_filtro.filter(('SESTACION===primero))

val replace_filtro2 = id_filtro2.select(substring(('FECHA),7,10).as("ANIO"), translate('PRECIPITACION, ",", ".").as("PRECIPITACION") )

val agrup2 = replace_filtro2.groupBy("ANIO")

val total = agrup2.agg(sum("PRECIPITACION").as("PRECIPITACION_TOTAL")).sort(asc("ANIO"))

total.show()







