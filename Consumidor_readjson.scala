import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.{col, from_json}
object Consumidor_readjson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Consumidor_readjson").master("local[2]").getOrCreate()
    //Conexión a Spark session. Se requieren dos conexiones para permitir streaming, de ahí el "[2]".
    val df = spark.readStream
      .format("kafka")
      //Indica formato de lectura del Stream
      .option("kafka.bootstrap.servers", "localhost:9092")
      //Especificaciones del puerto de conexión con el productor
      .option("subscribe", "readjson")
      //Indica el topic al que el consumidor debe suscribirse para obtener la información que le envía el productor
      .option("startingOffsets", "earliest") // From starting
      //Se establece el punto de inicio de lectura/consumo de los datos. Queremos que éste sea lo antes posible
      .load()
    val res = df.selectExpr("CAST(value AS STRING)")
    //Transformación (casteo) de formato kafka a string del contenido que recibe el consumidor
    val schema = new StructType()
      //Definición de una nueva estructura para los datos obtenidos. Se generan claves y se determina el tipo que tienen sus valores correspondientes
      .add("id",IntegerType)
      .add("first_name",StringType)
      .add("last_name",StringType)
      .add("email",StringType)
      .add("gender",StringType)
      .add("ip_address",IntegerType)
    //Estos seis nombres coinciden con los seis datos registrados para cada uno de los registros en el json "personal.json" a leer por el consumidor
    val Query = res.select(from_json(col("value"), schema).as("data"))
      //Con esto, a todos los valores recuperados del json se les apliquará el schema previamente definido. El alias "data" sirve para darle un nombre identificable con el fin de utilizarlo para futuras consultas
      .select("data.*").filter("first_name != 'Jeanette'").filter("first_name != 'Giavani'")
      //Query para la visualización del nuevo schema y la eliminación de la información de dos campos en la columna "nombre": 'Jeanette' y 'Giavani'
    Query.writeStream
      .format("console")
      //Indica el formato de escritura de los datos obtenidos por el consumidor
      .outputMode("append")
      //Los datos se van mostrando a medida que el consumidor los recibe
      .start()
      //Señal de inicio del consumo de datos
      .awaitTermination()
     //Instruye a esperar a que el consumidor se mantenga abierto hasta que deje de recibir datos
  }
}
