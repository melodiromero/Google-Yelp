var path = ("gs://top-vial-398602/Python_Datasets_gmaps_reviews.parquet")
var r_gmaps = spark.read.parquet(path).withColumn("datetime", from_unixtime($"time" / 1000).cast("timestamp")).withColumn("año", year($"datetime"))
var path = ("gs://top-vial-398602/Python_gmaps_metada.parquet")
var meta_gmaps = spark.read.parquet(path)
r_gmaps.printSchema()

var r_gmaps = r_gmaps.filter($"año" >= 2018)
println((r_gmaps_f.count(), r_gmaps_f.columns.length))
println((meta_gmaps.count(), meta_gmaps.columns.length))
meta_gmaps.select("category").show

import org.apache.spark.sql.functions._

val categorias = meta_gmaps.withColumn("exploded_category", explode($"category"))
val categoryCountsDf = categorias.groupBy("exploded_category").agg(count("*").alias("count"))
val sortedCategoryCountsDf = categoryCountsDf.orderBy(desc("count"))
val categoriasUnicas = sortedCategoryCountsDf.collect().map(row => (row.getString(0), row.getLong(1)))

var outputPath = "gs://top-vial-398602/Output-tables/categorias"
sortedCategoryCountsDf
  .write
  .csv(outputPath)// Se realiza el output de las categorías y numero de ocurrencia de las mismas

println((categorias.count(), categoriascolumns.length))  //En esta tabla necesito sacar las categorias

// Vamos a dejar un nuevo dataframe solo con las categorías de interés.

var categoriasPermitidas = List("restaurants", "Bakery", "Seafood", "Cafe", "Ice cream shop", "Chinese restaurant", "Bar & grill")
var g_mapsfiltrado = categorias.filter($"exploded_category".isin(categoriasPermitidas: _*))

val gmaps_reviews_metadata = r_gmaps.join(g_mapsfiltrado, Seq("gmap_id"), "inner")
println((gmaps_reviews_metadata.count(), gmaps_reviews_metadata.columns.length))

gmaps_reviews_metadata.write.csv("gs://top-vial-398602/Output-tables/gmaps_filtrado_joined")
scala> println((gmaps_reviews_metadata.count(), gmaps_reviews_metadata.columns.length))
(2882196,25)

-----------------------------------------------------------------------------------------------------------
import org.apache.spark.sql.DataFrame
var path = ("gs://top-vial-398602/Python_gmaps_metada.parquet")
var meta_gmaps: DataFrame = spark.read.parquet(path)
import org.apache.spark.sql.functions._
// Define una función UDF para extraer el estado
val extractStateUdf = udf((address: String) => {
  if (address != null) {
    val addressElements = address.split(",")
    if (addressElements.length >= 2) {
      val stateElement = addressElements.last.trim
      stateElement.take(2).toUpperCase
    } else {
      null // Devuelve null si no se pueden extraer las dos letras del estado
    }
  } else {
    null // Devuelve null si la dirección es nula
}
})
meta_gmaps = meta_gmaps.withColumn("us_state", extractStateUdf($"address"))

meta_gmaps = meta_gmaps.na.drop(Seq("category")) //Nueva inclusión
val categorias = meta_gmaps.withColumn("exploded_category", explode($"category"))
val categoryCountsDf = categorias.groupBy("exploded_category").agg(count("*").alias("count"))
val sortedCategoryCountsDf = categoryCountsDf.orderBy(desc("count"))
val categoriasUnicas = sortedCategoryCountsDf.collect().map(row => (row.getString(0), row.getLong(1)))
var categoriasPermitidas = List("Restaurant", "Bar", "bakery", "seafood", "Fast food restaurant", "Cafe", "Hamburger Restaurant",
                  "Family Restaurant","ice cream shop", "Chinese restaurant", "Italian restaurant", "Vegetarian Restaurant"
                   "Japanese restaurant", "Bistro", "Mexican restaurant", "Grill", "Dessert Restaurant"
                   "Greek restaurant", "Thai restaurant", "Indian restaurant", "Spanish restaurant",
                   "British restaurant", "Tex-mex restaurant", "American restaurant",
                   "Health food restaurant", "Delivery restaurant", "Brunch restaurant",
                   "Sandwich shop", "Fast food restaurant", "Cake shop", "Chicken restaurant",
                   "Donut shop", "Coffee shop", "Southern restaurant (US)", "Juice shop", "Breakfast restaurant",
                   "Pizza restaurant", "Bar & grill, Burrito restaurant", "Pizza", "Diner", "Takeout restaurant",
                   "Sushi restaurant", "Hamburger restaurant", "Steak house", "Asian restaurant", "New American restaurant", "Singaporean Restaurant")
var g_mapsfiltrado = categorias.filter($"exploded_category".isin(categoriasPermitidas: _*))
var path = ("gs://top-vial-398602/Python_Datasets_gmaps_reviews.parquet")
var r_gmaps = spark.read.parquet(path).withColumn("datetime", from_unixtime($"time" / 1000).cast("timestamp")).withColumn("año", year($"datetime"))
r_gmaps = r_gmaps.filter($"año" >= 2018)
r_gmaps = r_gmaps.withColumnRenamed("name", "user_name")
val extractState = udf((address: String) => {
  val addressElements = address.split(",").map(_.trim)
  if (addressElements.length >= 2) {
    val stateElement = addressElements(addressElements.length - 2)
    val stateParts = stateElement.split("\\s+")
    if (stateParts.nonEmpty) {
      stateParts(0)
    } else {
      "" // Devuelve una cadena vacía si no se encuentra un estado válido
    }
  } else {
    "" // Devuelve una cadena vacía si no se pueden dividir los elementos
  }
})
meta_gmaps = meta_gmaps.withColumn("us_state", extractState(col("address")))

var gmaps_reviews_metadata: DataFrame = r_gmaps.join(g_mapsfiltrado, Seq("gmap_id"), "inner")
gmaps_reviews_metadata = gmaps_reviews_metadata.drop("pics")
gmaps_reviews_metadata = gmaps_reviews_metadata.drop("MISC")
gmaps_reviews_metadata = gmaps_reviews_metadata.drop("description")
println((gmaps_reviews_metadata.count(), gmaps_reviews_metadata.columns.length))
spark.conf.set("spark.sql.files.maxRecordsPerFile", 5000000)
gmaps_reviews_metadata.write.parquet("gs://top-vial-398602/Output-tables/gmaps_filtrado_joined")

