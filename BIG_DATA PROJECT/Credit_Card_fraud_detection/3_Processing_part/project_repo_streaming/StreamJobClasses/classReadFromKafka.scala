
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DoubleType

object classReadFromKafka extends App{
   Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark=SparkSession.builder().master("local[2]").appName("streamingApp").getOrCreate();
  
  val tranxns=spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "cctxnstopicashnafaris")
  .option("startingOffsets", "latest")
  .load()

  
 val tranxnsString = tranxns.selectExpr("CAST(value AS STRING)","timestamp")
 
 val schema = new StructType()
      .add("card_id",LongType)
      .add("member_id",LongType)
      .add("amount",DoubleType)
      .add("pos_id",IntegerType)
      .add("post_code",IntegerType)
      .add("transc_dt",StringType)
      //.add("status",StringType)
      
   val tranxnsDF = tranxnsString.select(from_json(col("value"), schema).alias("value"),col("timestamp")).select("value.*","timestamp")
   
     /*tranxnsDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()*/
      
    /*  
    .writeStream.foreachBatch{(tranxnsDF,batchId)=> {
      println("Test") 
      println(batchId) }
    }.start().awaitTermination()
    */
    
    tranxnsDF.writeStream.foreachBatch{(bachdf,batchId)=> 
    { bachdf.foreach { myRow =>
      { 
        val myObj = new classCardValidation(); 
        println("Batchid : "+ batchId)
        var myArray = myRow.mkString(",").split(",")
        myObj.CardValidation(myArray)   
        println("success :"+ batchId)
      }
    }
	 }
  }.start().awaitTermination()
 
    
    /*var cardIdVal = myArray(0)
    var memberIdVal = myArray(1)
    var txnAmountVal = myArray(2)
    var posIdVal = myArray(3)
    var postCodeVal = myArray(4)
    var currTxnTimeVal = myArray(5)*/

    //val myObj = new classCardValidation()
    //val myVal  = Array("22222","22222","50000000","1212","10001","2021-02-01 19:19:41")
    //myObj.CardValidation(myArray)    
    
}


/*
{"card_id": 917221245657777,"member_id": 37495064475648584,"amount": 9000.567,"pos_id": 33946,"post_code":3946,"transc_dt":"11/02/2020"}
{"card_id": 917221245657778,"member_id": 37495064475648584,"amount": 9000.567,"pos_id": 33946,"post_code":3946,"transc_dt":"11/02/2020"}
{"card_id": 917221245657757,"member_id": 37495064475648584,"amount": 9000.567,"pos_id": 33946,"post_code":3946,"transc_dt":"11/02/2020"}

*/

//./kafka-server-start.sh ../config/server.properties
//./kafka-topics.sh  --create --topic cctxnstopic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

//./kafka-console-producer.sh --broker-list localhost:9092 --topic cctxnstopic

//./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cctxnstopic --from-beginning

/*object classReadFromKafka  extends App {
  
}
*/