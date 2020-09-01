package kvstore

import akka.actor.{Actor, ActorRef, Props, Timers}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.util.Success

object Replicator {

  case class Replicate(key: String, valueOption: Option[String], id: Long)

  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  case object RetryPendingSnapshot

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with Timers {

  import Replicator._
  import context.dispatcher

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

  timers.startTimerAtFixedRate("retryPendingSnapshots", RetryPendingSnapshot, 100 milliseconds)


  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case op: Replicate => handleReplicate(op, sender)
    case ack: SnapshotAck => handleSnapshotAck(ack)
    case RetryPendingSnapshot if pendingAcks.nonEmpty => retryPendingSnapshots()
  }

  private def handleReplicate(op: Replicate, client: ActorRef): Unit = {
    val sequence = nextSeq()
    pendingAcks += (sequence -> (client, op))
    replica ! Snapshot(op.key, op.valueOption, sequence)
    println(s"Sequence del replicate:$sequence")
  }

  private def retryPendingSnapshots(): Unit = {
    pendingAcks.foreach { case (sequence, (_, replicateMsg)) =>
      replica ! Snapshot(replicateMsg.key, replicateMsg.valueOption, sequence)
    }
  }

  private def handleSnapshotAck(ack: SnapshotAck): Unit = {
    println("paso")
    println(s"Sequence del ack:${ack.seq}")

    pendingAcks.get(ack.seq).foreach { case (client, _) =>
      println(client)
      client ! Replicated(ack.key, ack.seq)
      println("juli")
      pendingAcks -= ack.seq
    }
  }


}
