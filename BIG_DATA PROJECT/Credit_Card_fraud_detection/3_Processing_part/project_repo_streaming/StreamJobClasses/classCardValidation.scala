
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.filter
import org.apache.hadoop.hbase.client.{HBaseAdmin,
Result,Put,HTable,ConnectionFactory,Connection,Get,Scan}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import distanceFinder.distanceFinder
import distanceFinder._
import java.util.Calendar
import java.text.SimpleDateFormat
import java.sql.Date
import java.util.concurrent.TimeUnit

class classCardValidation {
  
   def CardValidation(requstParam : Array[String]) : String={
  //def CardValidation(requstParam : String) : String={
    var cardIdVal = requstParam(0)
    var memberIdVal = requstParam(1)
    var txnAmountVal = requstParam(2)
    var posIdVal = requstParam(3)
    var postCodeVal = requstParam(4)
    var currTxnTimeVal = requstParam(5)
    
    
    println("From ValidationClass:" + requstParam)
    
    /*val strInput = requstParam.split(",")
    
    var cardIdVal = strInput(0)
    var memberIdVal = strInput(1)
    var txnAmountVal = strInput(2)
    var posIdVal = strInput(3)
    var postCodeVal = strInput(4)
    var currTxnTimeVal = strInput(5)*/
    
    val sparkConf= new SparkConf().setAppName("Credit_Card_project").setMaster("local[2]") 
    val conf = HBaseConfiguration.create()
 
        
  conf.set("hbase.zookeeper.quoram","localhost")
  conf.set("hbase.zookeeper.property.clientPort","2181")

  val connection: Connection = ConnectionFactory.createConnection(conf)
  
  val tblCardLookup = connection.getTable(TableName.valueOf("card_lookup"))
  val tblCardTransaction = connection.getTable(TableName.valueOf("card_transactions"))
  
  println("Tables connected")
  
  var rowKeyOfLookUp =Bytes.toBytes(memberIdVal)
  
  //var myfilter = new PrefixFilter()
  
  var getOfLookUp = new Get(rowKeyOfLookUp)
    
  val rowOfLookUp = tblCardLookup.get(getOfLookUp)  
  
  //val scan = new Scan(rowKeyOfLookUp)
   println("Row Fetched From Hbase")  
   
  val last_txn_zip = rowOfLookUp.getValue(Bytes.toBytes("lkp_data"),Bytes.toBytes("last_txn_zip"))
  val last_txn_time = rowOfLookUp.getValue(Bytes.toBytes("lkp_data"),Bytes.toBytes("last_txn_time"))
  val score = rowOfLookUp.getValue(Bytes.toBytes("lkp_data"),Bytes.toBytes("score"))
  val ucl = rowOfLookUp.getValue(Bytes.toBytes("lkp_data"),Bytes.toBytes("ucl"))
  
  //val key_val  = Bytes.toString(rowKeyOfLookUp)
  println("Values extracted to local from hbase")  
  
  
  var last_txn_zipVal = Bytes.toString(last_txn_zip)
  var last_txn_timeVal = Bytes.toString(last_txn_time)
  var scoreVal = Bytes.toString(score) 
  var uclVal = Bytes.toString(ucl) 
  var status ="GENUINE"
  
  println("Bytes converted into String"+ scoreVal)
  
  if (scoreVal  == null )
  {
    scoreVal ="200"
  }
  if (scoreVal.toDouble < 200) 
    {
      status ="FRAUD"
    
    }
   println("Validation 1 is done") 
   
   println("txnAmountVal :"+ txnAmountVal)
   println("uclVal :"+ uclVal)
   
   
   if (uclVal == null )
   {
     uclVal = "9"
     status ="GENEUNE"
   }  
   else if (txnAmountVal.toDouble  > uclVal.toDouble) 
   {
   status ="FRAUD"   
   }
  println("Validation 2 is done") 
  var path ="/home/cloudera/Desktop/shareda/zipCodePosId.csv"

  
  var disUtil1New = new distanceFinder(path)

  if(last_txn_zipVal == null)
  {
    last_txn_zipVal = "10001"
  }
  if(postCodeVal == null)
  {
    posIdVal = "10301"
  }
  
  var distance = disUtil1New.getDistanceViaZipCode("10001", "10301")
  //var distance = disUtil1New.getDistanceViaZipCode(last_txn_zipVal, posIdVal)
  
  println("Distance Extracted :" + distance) 
  
  //calc time difference
  val sdf = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss")
  
  if  (last_txn_timeVal == null)
  {
  last_txn_timeVal ="2016-01-01 17:19:41"
  }
  
  val ts_last_txn = sdf.parse(last_txn_timeVal)
  val ts_curr_txn = sdf.parse(currTxnTimeVal)
  
  println("Time conversion done")
  
  val diff_in_time =  ts_curr_txn.getTime - ts_last_txn.getTime
  
  val diff_in_hrs = diff_in_time/(1000*60*60)
  
  println("time difference in hours :"+diff_in_hrs)
  var velocity = distance/diff_in_hrs
  
   if (velocity > 1000 )
   {
     status ="FRAUD"
   }
  println("velocity :" + velocity)
  println("Validation is completed")
  println("memberIdVal:"+ memberIdVal)
  println("uclVal:"+uclVal )
  println("scoreVal:"+ scoreVal)
  println("last_txn_timeVal:"+last_txn_timeVal )
  println("postCodeVal:"+postCodeVal )
  
  //insert into card_lookup is started
   val rowLookUP = new Put(Bytes.toBytes(memberIdVal))
    rowLookUP.addColumn(Bytes.toBytes("lkp_data"),Bytes.toBytes("member_id"),Bytes.toBytes(memberIdVal))
    rowLookUP.addColumn(Bytes.toBytes("lkp_data"),Bytes.toBytes("ucl"),Bytes.toBytes(uclVal))
    rowLookUP.addColumn(Bytes.toBytes("lkp_data"),Bytes.toBytes("score"),Bytes.toBytes(scoreVal))
    rowLookUP.addColumn(Bytes.toBytes("lkp_data"),Bytes.toBytes("last_txn_time"),Bytes.toBytes(currTxnTimeVal))
    rowLookUP.addColumn(Bytes.toBytes("lkp_data"),Bytes.toBytes("last_txn_zip"),Bytes.toBytes(postCodeVal))
    
       
    tblCardLookup.put(rowLookUP)  
    
    println("Cardlookup is updated")
    
  //insert into card_lookup is ends
  
  var tran_key = cardIdVal + currTxnTimeVal
  
  val rowCardTxns = new Put(Bytes.toBytes(tran_key))
  rowCardTxns.addColumn(Bytes.toBytes("trans_data"),Bytes.toBytes("member_id"),Bytes.toBytes(memberIdVal))
  rowCardTxns.addColumn(Bytes.toBytes("trans_data"),Bytes.toBytes("card_id"),Bytes.toBytes(cardIdVal))
  rowCardTxns.addColumn(Bytes.toBytes("trans_data"),Bytes.toBytes("amount"),Bytes.toBytes(txnAmountVal))
  rowCardTxns.addColumn(Bytes.toBytes("trans_data"),Bytes.toBytes("transaction_dt"),Bytes.toBytes(currTxnTimeVal))
  rowCardTxns.addColumn(Bytes.toBytes("trans_data"),Bytes.toBytes("postcode"),Bytes.toBytes(postCodeVal))
  rowCardTxns.addColumn(Bytes.toBytes("trans_data"),Bytes.toBytes("pos_id"),Bytes.toBytes(posIdVal))
  rowCardTxns.addColumn(Bytes.toBytes("trans_data"),Bytes.toBytes("status"),Bytes.toBytes(status))
  
  tblCardTransaction.put(rowCardTxns)
  
  println("CardTransactions is updated")
  
  //card_transactions_h insert is ends    
   println("========"+status+"=========")
   status
  }
}

/*
object classCardValidation extends App {
  
  
  println("Process is started")
      
  val myVal  = Array("11111","11111","50000000","1212","10001","2021-02-01 19:19:41")
  
  val obj  = new classCardValidation()
  
  println("Object Created")
  
  var status = obj.CardValidation(myVal)
  
  println("Process is completed successfully")
  
  //get 'card_transactions_h1','111112021-02-01 19:19:41'
  //scan 'card_transactions_h1',{FILTER,"ValueFilter(=,'binary:11111')"}
  
}
*/

