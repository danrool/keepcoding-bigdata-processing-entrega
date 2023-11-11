// Databricks notebook source
val sc = spark.sparkContext

import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.functions.col

val schema2021 = StructType(Array(
    StructField("CountryName", StringType, true),
    StructField("RegionalIndicator", StringType, true),
    StructField("LadderScore", DoubleType, true),
    StructField("StandardErrorOfLadderScore", DoubleType, true),
    StructField("UpperWhisker", DoubleType, true),
    StructField("LowerWhisker", DoubleType, true),
    StructField("LoggedGDPPerCapita", DoubleType, true),
    StructField("SocialSupport", DoubleType, true),
    StructField("HealthyLifeExpectancy", DoubleType, true),
    StructField("FreedomToMakeLifeChoices", DoubleType, true),
    StructField("Generosity", DoubleType, true),
    StructField("PerceptionsOfCorruption", DoubleType, true),
    StructField("LadderScoreInDystopia", DoubleType, true),
    StructField("ExplainedByLogGDPPerCapita", DoubleType, true),
    StructField("ExplainedBySocialSupport", DoubleType, true),
    StructField("ExplainedByHealthyLifeExpectancy", DoubleType, true),
    StructField("ExplainedByFreedomToMakeLifeChoices", DoubleType, true),
    StructField("ExplainedByGenerosity", DoubleType, true),
    StructField("ExplainedByPerceptionsOfCorruption", DoubleType, true),
    StructField("DystopiaResidual", DoubleType, true)))

val schemaAll = StructType(Array(
    StructField("CountryName", StringType, true),
    StructField("Year", IntegerType, true),
    StructField("LifeLadder", DoubleType, true),
    StructField("LogGDPPerCapita", DoubleType, true),
    StructField("SocialSupport", DoubleType, true),
    StructField("HealthyLifeExpectancyAtBirth", DoubleType, true),
    StructField("FreedomToMakeLifeChoices", DoubleType, true),
    StructField("Generosity", DoubleType, true),
    StructField("PerceptionsOfCorruption", DoubleType, true),
    StructField("PositiveAffect", DoubleType, true),
    StructField("NegativeAffect", DoubleType, true),
    StructField("HappynessIndex", DoubleType, true)))


val df2 = spark.read
            .option("header", "true")
            .schema(schema2021)
            .csv(path="dbfs:/FileStore/datasets/world_happiness_report_2021.csv")

val dfa = spark.read
            .option("header", "true")
            .schema(schemaAll)
            .csv(path="dbfs:/FileStore/datasets/world_happiness_report.csv")


//df2.show
//dfa.show


// COMMAND ----------

// Creación de las vistas temporale para la ejecucion de consultas SQL

df2.createOrReplaceTempView("df_2021")
dfa.createOrReplaceTempView("df_all")

// COMMAND ----------

//
// Ejercicio 1 - ¿Cuál es el país más “feliz” del 2021 según la data? (considerar que la columna “Ladder score” 
//               mayor número más feliz es el país)

// COMMAND ----------

// MAGIC %sql
// MAGIC select concat('El país mas felis es : ', CountryName) from df_2021 order by LadderScore Desc Limit 1
// MAGIC
// MAGIC

// COMMAND ----------

//
// Ejercicio 2 - ¿Cuál es el país más “feliz” del 2021 por continente según la data
//

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT
// MAGIC   RegionalIndicator,
// MAGIC   CountryName,
// MAGIC   LadderScore
// MAGIC FROM (
// MAGIC   SELECT
// MAGIC     CountryName,
// MAGIC     RegionalIndicator,
// MAGIC     LadderScore,
// MAGIC     ROW_NUMBER() OVER (PARTITION BY RegionalIndicator ORDER BY LadderScore DESC) as Ranking
// MAGIC   FROM
// MAGIC     df_2021
// MAGIC ) ranked
// MAGIC WHERE
// MAGIC   Ranking = 1
// MAGIC ORDER BY RegionalIndicator
// MAGIC

// COMMAND ----------

//
// Ejercicio 3 - ¿Cuál es el país que más veces ocupó el primer lugar en todos los años?
//

// COMMAND ----------

// Concateno los dataset para obtener las conclusion sobre el universo de datos
val df_2021 = df2.withColumn("year", lit(2021)).withColumnRenamed("LadderScore", "LifeLadder").select("CountryName","Year","LifeLadder")
//df_2021.show()

val df_all_data = df_2021.union(dfa.select("CountryName","Year","LifeLadder"))
//df_all_data.show(1000)

df_all_data.createOrReplaceTempView("df_all_data")


// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT concat('País : ', CountryName , " el ranking " , count(*) ) AS Ejercicio_3
// MAGIC FROM (
// MAGIC   SELECT 
// MAGIC     CountryName, 
// MAGIC     Year, 
// MAGIC     LifeLadder,
// MAGIC     ROW_NUMBER() OVER (PARTITION BY Year ORDER BY LifeLadder DESC) as Ranking
// MAGIC   FROM 
// MAGIC     df_all_data
// MAGIC ) 
// MAGIC WHERE Ranking = 1
// MAGIC GROUP BY CountryName
// MAGIC ORDER BY COUNT(*) DESC
// MAGIC

// COMMAND ----------

//
// Ejercicio 4 - ¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?
//

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT concat('El País con mayor GDP es: ', CountryName, ' ocupa la posicion # ', Ranking , ' de felicidad') AS Ejercicio_4
// MAGIC FROM (
// MAGIC   SELECT 
// MAGIC     CountryName, 
// MAGIC     Year, 
// MAGIC     LifeLadder,
// MAGIC     LogGDPPerCapita,
// MAGIC     ROW_NUMBER() OVER (PARTITION BY Year ORDER BY LifeLadder DESC) as Ranking
// MAGIC   FROM 
// MAGIC     df_all
// MAGIC   WHERE Year = 2020
// MAGIC ) 
// MAGIC ORDER BY LogGDPPerCapita DESC
// MAGIC LIMIT 1

// COMMAND ----------

//
// Ejercicio 5 - ¿En que porcentaje a variado a nivel mundial el GDP promedio del 2020 respecto al 2021? ¿Aumentó 
//                o disminuyó?

// COMMAND ----------

var avg_2020: Double = 0.0
var avg_2021: Double = 0.0
var resultadoPunto5: Double = 0.0

avg_2020 = dfa.filter(col("year")===2020).select(avg(col("LogGDPPerCapita"))).head().getAs[Double](0)

avg_2021 = df2.select(avg(col("LoggedGDPPerCapita"))).head().getAs[Double](0)


resultadoPunto5 = ((avg_2021 - avg_2020) / avg_2020) * 100
print("El porcentage de variación de GDP es " + resultadoPunto5.toString + "%, por lo que ", if(resultadoPunto5 < 0) "disminuyo" else "aumento")


// COMMAND ----------

//
// Ejercicio 6 - ¿Cuál es el país con mayor expectativa de vide (“Healthy life expectancy at birth”)? Y ¿Cuánto tenia 
//                en ese indicador en el 2019?

// COMMAND ----------

var resultado : String = ""

// Concateno los dataset para obtener las conclusion sobre el universo de datos
val dfa_all = dfa.withColumnRenamed("HealthyLifeExpectancyAtBirth", "HealthyLifeExpectancy").select("CountryName","Year","HealthyLifeExpectancy")

val df_2021 = df2.withColumn("year", lit(2021)).select("CountryName","Year","HealthyLifeExpectancy")
//df_2021.show()

val df_all_data = df_2021.union(dfa_all.select("CountryName","Year","HealthyLifeExpectancy"))

// Calculo el pais con mayor espectativa de vida
val dfResultado  = df_all_data.select("CountryName").orderBy(col("HealthyLifeExpectancy").desc).limit(1) 
println("El pais con mayor espectativa de vide historircamente es ")
dfResultado.show()
resultado = dfResultado.head.getAs[String](0)

// Calculo el valor para 2019
val dfResultado2019 = df_all_data.filter( (col("Year") === 2019) && (col("CountryName") === resultado) ).select("CountryName", "HealthyLifeExpectancy")

println ("En 2019 ", resultado ," tenia este valor de espectativa de vida");
dfResultado2019.show()



