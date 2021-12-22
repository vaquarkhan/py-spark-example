import org.apache.spark._
import kafka.serializer.StringDecoder
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._


// 스파크 스트리밍 컨텍스트 생성

val ssc = new StreamingContext(sc, Seconds(5))


// 카프카 설정을 Map 객체로 생성

val kafkaReceiverParams = Map[String, String](
  "metadata.broker.list" -> "34.123.104.28:9092")

// DStream을 생성한다.

val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc, 
    kafkaReceiverParams, 
    Set("orders"))

// 카프카 orders 토픽용 객체(스키마) 생성

import java.sql.Timestamp
case class Order(time: java.sql.Timestamp, orderId:Long, clientId:Long,
  symbol:String, amount:Int, price:Double, buy:Boolean)

import java.text.SimpleDateFormat


// orders 카프카 토픽 데이터를 기반으로 데이터를 정제한다.

val orders = kafkaStream.flatMap(line => {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val s = line._2.split(",") // 20:25:28, 1, 80, EPE, 710, 51.00, B
    try {
      assert(s(6) == "B" || s(6) == "S") // B, S 만 포함될 수 있음. 다른 값이면 AssertionError 발생
        // 필드 값을 파싱하여 Order 객체 기반의 List를 리턴
      List(Order(new Timestamp(dateFormat.parse(s(0)).getTime()),
    s(1).toLong, s(2).toLong, s(3), s(4).toInt, s(5).toDouble, s(6) == "B"))
    }

    // 형식이 안 맞으면 빈 List 리턴
    catch {
      case e : Throwable => println("Wrong line format ("+e+"): "+line._2)
      List()
    }
})
   
// (true, 매수주문건수) , (false, 매도주문건수)
val numPerType = orders.map(o => (o.buy, 1L)).reduceByKey((c1, c2) => c1+c2)   

// ("BUYS", 매수주문건수 List) , ("SELLS", 매도주문건수 List)
val buySellList = numPerType.map(t =>
  if(t._1) ("BUYS", List(t._2.toString)) // numPerType의 키가 true이면 매수 주문 건수를 기록
  else ("SELLS", List(t._2.toString)) ) // false이면 매도 주문 건수를 기록

// key : 고객ID, value : 거래액(주문 수량 * 매매 가격) 으로 Pair DStream 생성
val amountPerClient = orders.map(o => (o.clientId, o.amount*o.price))

// key : 고객ID, value : 거래액 sum

val amountState = amountPerClient.updateStateByKey((vals, totalOpt:Option[Double]) => { // updateStateByKey(키-고객ID, 키의 상태값)
      totalOpt match {
        case Some(total) => Some(vals.sum + total) // 키의 상태가 이미 존재하면 상태값(total)에 새로 유입된 값의 합계(vals.sum)를 더한다.
        case None => Some(vals.sum) // 이전 상태 값이 없으면 새로 유입된 값의 합계만 반환한다.
      }
})

// 거래액 상위 1~5위 고객 (고객ID, 거래액)

val top5clients = amountState.transform(_.sortBy(_._2, false).map(_._1). //sortBy로 내림차순 정렬 -> map으로 번호는 제거
  zipWithIndex.filter(x => x._2 < 5)) // zipWithIndex로 RDD의 각 요소에 번호를 부여 -> 상위 5개 남기고 필터링

// "TOP5CLIENTS", 거래액 상위 1~5위 고객 List
val top5clList = top5clients.repartition(1). // 모든 데이터를 단일 파티션으로 모은다.
    map(x => x._1.toString). // 고객ID(x._1)만 남긴다(string)
    glom(). // 파티션에 포함된 모든 고객 ID를 단일 배열로 묶는다.
    map(arr => ("TOP5CLIENTS", arr.toList)) // 지표 이름과 고객 ID의 배열을 튜플로 만든다.
// 지난 1시간 동안 거래된 종목별 거래량 (종목명, 거래량 합)
val stocksPerWindow = orders.map(x => (x.symbol, x.amount)).
  reduceByKeyAndWindow((a1:Int, a2:Int) => a1+a2, Minutes(60))
//  "TOP5STOCKS", 거래액 상위 1~5위 종목 List
val topStocks = stocksPerWindow.transform(_.sortBy(_._2, false).map(_._1).
  zipWithIndex.filter(x => x._2 < 5)).repartition(1).
    map(x => x._1.toString).glom().
    map(arr => ("TOP5STOCKS", arr.toList))
// 최종 input data (DStream)
// "BUYS", "SELLS", "TOP5CLIENTS", "TOP5STOCKS"
val finalStream = buySellList.union(top5clList).union(topStocks)

// Producer 싱글톤 객체 (JVM별로  Producer 객체를 하나씩만 초기화)

// 카프카 브로커에 접속 -> KeyedMessage 객체의 형태로 구성한 메시지를 카프카 토픽으로 전송

// 별도 jar 파일 사용함 (spark-shell 실행시 jar 옵션 추가)

import org.sia.KafkaProducerWrapper

finalStream.foreachRDD((rdd) => {
      rdd.foreachPartition((iter) => { // RDD 파티션별로 Producer를 하나씩 생성한다. (KafkaProducerWrapper는 JVM 별로 Producer 객체를 하나씩만 초기화 하는디..대치되는 개념이 아닌가?)

        KafkaProducerWrapper.brokerList = "104.197.13.243:9092"
        val producer = KafkaProducerWrapper.instance // KafkaProducerWrapper 인스턴스 생성
        iter.foreach({ case (metric, list) =>
          producer.send("spark", metric, list.toString) }) // input data를 metrics 카프카 토픽에 전송한다.(producer)
      })
})
sc.setCheckpointDir("/home/freud/checkpoint")
ssc.start()