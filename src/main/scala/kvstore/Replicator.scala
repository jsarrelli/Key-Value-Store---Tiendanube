package kvstore

import akka.actor.{Actor, ActorRef, Props, Timers}
import akka.event.Logging.InfoLevel
import akka.event.LoggingReceive

import scala.concurrent.duration._
import scala.language.postfixOps

object Replicator {

  case class Replicate(key: String, valueOption: Option[String], id: Long)

  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  case class RetryPendingSnapshot(seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with Timers {

  import Replicator._

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var pendingAcks = Map.empty[Long, (ActorRef, Replicate)]

  var _seqCounter = 0L

  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  def receive: Receive = LoggingReceive(InfoLevel) {
    case op: Replicate => handleReplicate(op, sender)
    case ack: SnapshotAck => handleSnapshotAck(ack)
    case msg: RetryPendingSnapshot => retryPendingSnapshots(msg)
  }

  private def handleReplicate(op: Replicate, client: ActorRef): Unit = {
    val sequence = nextSeq()
    pendingAcks += (sequence -> (client, op))
    replica ! Snapshot(op.key, op.valueOption, sequence)
    val retryPendingMsg = RetryPendingSnapshot(sequence)
    timers.startTimerAtFixedRate(s"retryPendingSnapshot-$sequence", retryPendingMsg, 100 milliseconds)

  }

  private def retryPendingSnapshots(msg: RetryPendingSnapshot): Unit = {
    pendingAcks.get(msg.seq) foreach { case (_, replicate) =>
      replica ! Snapshot(replicate.key, replicate.valueOption, msg.seq)
    }
  }

  private def handleSnapshotAck(ack: SnapshotAck): Unit = {
    timers.cancel(s"retryPendingSnapshot-${ack.seq}")
    pendingAcks.get(ack.seq) foreach { case (client, op) =>
      client ! Replicated(ack.key, op.id)
    }
    pendingAcks -= ack.seq
  }


}
