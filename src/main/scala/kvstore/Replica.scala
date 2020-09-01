package kvstore

import akka.actor.SupervisorStrategy.Resume
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props, SupervisorStrategy, Timers}
import akka.event.Logging.InfoLevel
import akka.event.LoggingReceive
import kvstore.Arbiter._
import kvstore.Replicator.{Replicate, Replicated, Snapshot, SnapshotAck}

import scala.collection.{immutable, mutable}
import scala.concurrent.duration._
import scala.language.postfixOps

object Replica {

  sealed trait Operation {
    def key: String

    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case object RetryPending

  case class RetryPersistence(key: String, value: Option[String], id: Long)

  case class CheckGlobalAck(msgId: Long, client: ActorRef)


  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with Timers {

  import Persistence._
  import Replica._

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  arbiter ! Join

  //timers.startTimerAtFixedRate("retryPendingSnapshots", RetryPending, 100 milliseconds)

  val persistentService: ActorRef = context.actorOf(persistenceProps)

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var expectedSequence: Long = 0

  /* message Id for all the messages pending for persistence, and actor ref of the client
   id -> client
  * */
  var pendingPersistedAck = Map.empty[Long, ActorRef]
  /*  message Id and the replicas we are waiting for ack
  *  id -> (secondary replicator, client) */
  var pendingReplicatesAck = Map.empty[Long, mutable.Set[(ActorRef, ActorRef)]]

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy(withinTimeRange = 1 second) {
    case _: PersistenceException => Resume
  }

  override def receive: Receive = LoggingReceive(InfoLevel) {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  val leader: Receive = LoggingReceive(InfoLevel) {
    case msg: Insert => handleUpdateMsg(msg, sender)
    case msg: Remove => handleUpdateMsg(msg, sender)
    case msg: Get => handleGet(msg, sender)
    case msg: Persisted => handlePersistedLeader(msg)
    case msg: RetryPersistence => handleRetryPersistence(msg)
    case msg: Replicated => handleReplicated(msg, sender)
    case msg: CheckGlobalAck => handleCheckGlobalAck(msg)
    case Replicas(replicas) => handleReplicasMsg(replicas)
  }

  val replica: Receive = LoggingReceive(InfoLevel) {
    case msg: Get => handleGet(msg, sender)
    case msg: Snapshot => handleSnapshot(msg, sender)
    case msg: Persisted => handlePersistedReplica(msg)
    case msg: RetryPersistence => handleRetryPersistence(msg)
  }

  private def handleUpdateMsg(op: Operation, sender: ActorRef): Unit = {
    val persistMsg = op match {
      case Insert(key, value, id) =>
        kv += (key -> value)
        Persist(key, Some(value), id)
      case Remove(key, id) =>
        kv -= key
        Persist(key, None, id)
    }
    pendingPersistedAck += (persistMsg.id -> sender)

    persistentService ! persistMsg
    replicateToAllReplicas(op)

    val retryPersistenceMsg = RetryPersistence(persistMsg.key, persistMsg.valueOption, persistMsg.id)
    timers.startSingleTimer(s"checkGlobalAck+${persistMsg.id}", CheckGlobalAck(persistMsg.id, sender), 1 seconds)
    timers.startTimerAtFixedRate(s"retryPersistence+${persistMsg.id}", retryPersistenceMsg, 100 milliseconds)
  }

  private def replicateToAllReplicas(op: Operation): Unit = {
    val replicateMsg = op match {
      case Insert(key, value, id) => Replicate(key, Some(value), id)
      case Remove(key, id) => Replicate(key, None, id)
    }
    if (replicators.nonEmpty)
      pendingReplicatesAck += (replicateMsg.id -> replicators.map((_, sender)).to(collection.mutable.Set))

    replicators foreach (_ ! replicateMsg)
  }

  private def handleGet(op: Get, sender: ActorRef): Unit = {
    sender ! GetResult(op.key, kv.get(op.key), op.id)
  }

  private def handleRetryPersistence(msg: RetryPersistence): Unit = {
    persistentService ! Persist(msg.key, msg.value, msg.id)
  }

  private def handleSnapshot(msg: Snapshot, replicator: ActorRef): Unit = {
    msg.seq match {
      case seq if seq < expectedSequence =>
        sender ! SnapshotAck(msg.key, msg.seq)
      case seq if seq == expectedSequence =>
        msg.valueOption match {
          case Some(value) => kv += (msg.key -> value)
          case None => kv -= msg.key
        }
        pendingPersistedAck += (msg.seq -> replicator)
        persistentService ! Persist(msg.key, msg.valueOption, msg.seq)
        val retryPersistenceMsg = RetryPersistence(msg.key, msg.valueOption, msg.seq)
        timers.startTimerAtFixedRate(s"retryPersistence+${msg.seq}", retryPersistenceMsg, 100 milliseconds)
      case _ =>
    }
  }

  private def handleReplicasMsg(replicas: Set[ActorRef]): Unit = {
    val stoppedReplicas = (secondaries.keys.toSet + self).diff(replicas)
    val newReplicas = replicas.diff(secondaries.keys.toSet + self)
    val stoppedReplicators = secondaries.collect { case (replica, replicator) if stoppedReplicas.contains(replica) => replicator }.toSet

    //remove replicators from the pendingReplicateList, and acknowledge that message if theres no one else to wait for
    pendingReplicatesAck.foreach { case (key, pending) =>
      val elementsToRemove = pending.filter { case (secondary, _) => stoppedReplicators.contains(secondary) }
      pending --= elementsToRemove
      if (pending.isEmpty) {
        pendingReplicatesAck -= key
        elementsToRemove.headOption.foreach { case (_, client) => client ! OperationAck(key) }
      }
    }

    //remove stopped replicas and stops their replicator
    stoppedReplicas.foreach { replica =>
      secondaries.get(replica).foreach {
        replicator =>
          secondaries -= replica
          context.stop(replicator)
          replicators -= replicator
      }
    }

    //save new replicas and creates their replicator
    newReplicas.foreach { replica =>
      val replicator = context.actorOf(Replicator.props(replica))
      secondaries += (replica -> replicator)
      replicators += replicator
      kv.foreach { case (key, value) =>
        replicator ! Replicate(key, Some(value), 0)
      }
    }
  }

  private def handlePersistedLeader(persisted: Persisted): Unit = {
    timers.cancel(s"retryPersistence+${persisted.id}")
    pendingPersistedAck.get(persisted.id) foreach { client =>
      if (!pendingReplicatesAck.contains(persisted.id)) {
        timers.cancel(s"checkGlobalAck+${persisted.id}")
        client ! OperationAck(persisted.id)
      }
    }
    pendingPersistedAck -= persisted.id
  }

  private def handlePersistedReplica(persisted: Persisted): Unit = {
    timers.cancel(s"retryPersistence+${persisted.id}")
    pendingPersistedAck.get(persisted.id) foreach { client =>
      expectedSequence += 1
      client ! SnapshotAck(persisted.key, persisted.id)
    }
    pendingPersistedAck -= persisted.id
  }

  private def handleReplicated(replicated: Replicated, replicator: ActorRef): Unit = {
    pendingReplicatesAck.get(replicated.id).foreach { pendings =>
      pendings.find { case (client, _) => client == replicator }
        .foreach { element =>
          pendings -= element
          if (pendings.isEmpty) {
            pendingReplicatesAck -= replicated.id
            if (!pendingPersistedAck.contains(replicated.id)) {
              val client = element._2
              timers.cancel(s"checkGlobalAck+${replicated.id}")
              client ! OperationAck(replicated.id)
            }
          }
        }
    }
  }

  private def handleCheckGlobalAck(ack: CheckGlobalAck): Unit = {
    if (pendingReplicatesAck.contains(ack.msgId) || pendingPersistedAck.contains(ack.msgId))
      ack.client ! OperationFailed(ack.msgId)
    timers.cancel(s"retryPersistence+${ack.msgId}")
    pendingPersistedAck -= ack.msgId
    pendingReplicatesAck -= ack.msgId
  }

}

