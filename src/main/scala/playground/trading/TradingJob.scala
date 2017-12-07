package playground.trading

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object TradingJob {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val PositionPort = 2000
    val BidPort = 3000
    val TradePort = 4000
    val PositionHost = "localhost"
    val BidHost = "localhost"
    val TradeHost = "localhost"

    val positions = env.socketTextStream(PositionHost, PositionPort)
      .map(Position.fromString(_))
      .flatMap(BadDataHandler[Position])
      .keyBy(_.symbol)

    val bids = env.socketTextStream(BidHost, BidPort)
      .map(Bid.fromString(_))
      .flatMap(BadDataHandler[Bid])
      .keyBy(_.symbol)

    positions
      .connect(bids)
      .process(TradeEngine()).uid("TradeEngine")
      .map(_.toString + '\n')
      .writeToSocket(TradeHost, TradePort, new SimpleStringSchema)

    env.execute()
  }
}
