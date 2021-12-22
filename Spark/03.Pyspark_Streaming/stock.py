from __future__ import print_function
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from kafka.errors import KafkaError

ssc = StreamingContext(sc, 5)

kafkaReceiverParams = {"metadata.broker.list": "34.123.104.28:9092"}
kafkaStream = KafkaUtils.createDirectStream(ssc, ["orders"], kafkaReceiverParams)

from datetime import datetime
def parseOrder(line):
  s = line[1].split(",")
  try:
      if s[6] != "B" and s[6] != "S":
        raise Exception('Wrong format')
      return [{"time": datetime.strptime(s[0], "%Y-%m-%d %H:%M:%S"), "orderId": long(s[1]), "clientId": long(s[2]), "symbol": s[3],
      "amount": int(s[4]), "price": float(s[5]), "buy": s[6] == "B"}]
  except Exception as err:
      print("Wrong line format (%s): " % line)
      return []

orders = kafkaStream.flatMap(parseOrder)
from operator import add
numPerType = orders.map(lambda o: (o['buy'], 1L)).reduceByKey(add)

amountPerClient = orders.map(lambda o: (o['clientId'], o['amount']*o['price']))

amountState = amountPerClient.updateStateByKey(lambda vals, totalOpt: sum(vals)+totalOpt if totalOpt != None else sum(vals))
top5clients = amountState.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False).map(lambda x: x[0]).zipWithIndex().filter(lambda x: x[1] < 5))

buySellList = numPerType.map(lambda t: ("BUYS", [str(t[1])]) if t[0] else ("SELLS", [str(t[1])]) )
top5clList = top5clients.repartition(1).map(lambda x: str(x[0])).glom().map(lambda arr: ("TOP5CLIENTS", arr))

stocksPerWindow = orders.map(lambda x: (x['symbol'], x['amount'])).reduceByKeyAndWindow(add, None, 60*60)
stocksSorted = stocksPerWindow.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False).map(lambda x: x[0]).zipWithIndex().filter(lambda x: x[1] < 5))
topStocks = stocksSorted.repartition(1).map(lambda x: str(x[0])).glom().map(lambda arr: ("TOP5STOCKS", arr))

finalStream = buySellList.union(top5clList).union(topStocks)
finalStream.foreachRDD(lambda rdd: rdd.foreach(print))

def sendMetrics(itr):
  prod = KafkaProducerWrapper.getProducer(["34.123.104.28:9092"])
  for m in itr:
    prod.send("spark", key=m[0], value=m[0]+","+str(m[1]))
  prod.flush()

finalStream.foreachRDD(lambda rdd: rdd.foreachPartition(sendMetrics))

sc.setCheckpointDir("/home/freud/spark/checkpoint/")

ssc.start()