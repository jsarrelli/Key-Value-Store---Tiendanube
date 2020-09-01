package kvstore

import akka.actor
import akka.actor.SupervisorStrategy.Resume
import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Timers}
import akka.pattern.ask
import akka.util.Timeout
import kvstore.Arbiter._
import kvstore.Replicator.{Replicate, Replicated, Snapshot, SnapshotAck}

import scala.collection.immutable.{AbstractSet, SortedSet}
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.{Failure, Success}

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


  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with Timers {

  import Persistence._
  import Replica._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  arbiter ! Join

  timers.startTimerAtFixedRate("retryPendingSnapshots", RetryPending, 100 milliseconds)

  implicit val timeout = Timeout(1 seconds)

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var expectedSequence: Long = 0

  var pendingPersistedAck = Map.empty[Long, (ActorRef, Persist)]

  var pendingAcksFromSecondaries = Map.empty[Long, mutable.Set[ActorRef]]

  val persistentService: ActorRef = context.actorOf(persistenceProps)

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy(withinTimeRange = 1 second) {
    case _: PersistenceException => Resume
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case op: Insert => handleInsert(op, sender())
    case op: Remove => handleRemove(op, sender)
    case op: Get => handleGet(op, sender)
    case Replicas(replicas) => handleReplicasMsg(replicas)
    case RetryPending => handleRetryPendingUpdateAcks()
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case op: Get => handleGet(op, sender)
    case op: Snapshot if op.seq < expectedSequence =>
      println("entro")
      sender ! SnapshotAck(op.key, op.seq)
    case op: Snapshot if expectedSequence == op.seq =>
      println("entro")
      handleSnapshot(op, sender)
    case RetryPending => retryPendingSnapshotAcks()
  }

  private def handleInsert(op: Insert, sender: ActorRef) = {
    val persisMsg = Persist(op.key, Some(op.value), op.id)
    pendingPersistedAck += (op.id -> (sender, persisMsg))
    persisMsg.valueOption match {
      case Some(value) => kv += (persisMsg.key -> value)
      case None => kv -= op.key
    }
    (persistentService ? persisMsg).mapTo[Persisted].onComplete {
      case Failure(ex) =>
        sender ! OperationFailed(op.id)
        throw ex
      case Success(_) =>
        pendingPersistedAck -= persisMsg.id
        replicateToSecondaries(persisMsg, sender)
    }
  }

  private def handleRemove(op: Remove, sender: ActorRef) = {
    val persisMsg = Persist(op.key, None, op.id)
    pendingPersistedAck += (op.id -> (sender, persisMsg))
    (persistentService ? persisMsg).mapTo[Persisted].onComplete {
      case Failure(ex) =>
        sender ! OperationFailed(op.id)
        throw ex
      case Success(_) =>
        pendingPersistedAck -= op.id
        kv -= op.key
        replicateToSecondaries(persisMsg, sender)
    }
  }

  private def handleGet(op: Get, sender: ActorRef): Unit = {
    sender ! GetResult(op.key, kv.get(op.key), op.id)
  }

  private def handleSnapshot(op: Snapshot, replicator: ActorRef) = {
    op.valueOption match {
      case Some(value) => kv += (op.key -> value)
      case None => kv -= op.key
    }
    val persistMsg = Persist(op.key, op.valueOption, op.seq)
    pendingPersistedAck += (op.seq -> (replicator, persistMsg))
    (persistentService ? persistMsg).mapTo[Persisted] onComplete {
      case Success(_) =>
        replicator ! SnapshotAck(op.key, op.seq)
        expectedSequence += 1
        pendingPersistedAck -= op.seq
    }

  }

  private def handleReplicasMsg(replicas: Set[ActorRef]): Unit = {
    val stoppedReplicas = (secondaries.values.toSet + self).diff(replicas)
    val newReplicas = replicas.diff(secondaries.values.toSet + self)

    //remove stopped replicas and stops their replicator
    stoppedReplicas.foreach { replica =>
      secondaries.get(replica).foreach {
        replicator =>
          replicator ! PoisonPill
          replicators -= replicator
      }
      secondaries -= replica
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

  private def retryPendingSnapshotAcks(): Unit = {
    pendingPersistedAck.foreach { case (_, (replicator, persistMsg)) =>
      (persistentService ? Persist(persistMsg.key, persistMsg.valueOption, persistMsg.id)).mapTo[Persisted] onComplete {
        case Success(_) =>
          replicator ! SnapshotAck(persistMsg.key, persistMsg.id)
          expectedSequence += 1
          pendingPersistedAck -= persistMsg.id
        case _=>
      }
    }
  }

  private def handleRetryPendingUpdateAcks(): Unit = {
    pendingPersistedAck.foreach { case (_, (replicator, persistMsg)) =>
      (persistentService ? Persist(persistMsg.key, persistMsg.valueOption, persistMsg.id)).mapTo[Persisted] onComplete {
        case Success(_) =>
          replicator ! OperationAck(persistMsg.id)
          expectedSequence += 1
          pendingPersistedAck -= persistMsg.id
      }
    }
  }

  private def replicateToSecondaries(msg: Persist, client: ActorRef): Unit = {
    if (replicators.isEmpty) client ! OperationAck(msg.id)
    else {
      pendingAcksFromSecondaries += (msg.id -> replicators.to(collection.mutable.Set))
      replicators.foreach { replicator =>
        (replicator ? Replicate(msg.key, msg.valueOption, msg.id)).mapTo onComplete {
          case Success(msg) =>
            println("pasa")
            handleReplicatedAck(msg, replicator, client)
          case Failure(_) =>
            println("rompe")
            client ! OperationFailed(msg.id)
        }
      }
    }
  }

  private def handleReplicatedAck(msg: Replicated, replicator: ActorRef, client: ActorRef): Unit = {
    pendingAcksFromSecondaries.get(msg.id).foreach { pendingReplicators =>
      pendingReplicators -= replicator
      if (pendingReplicators.isEmpty) {
        pendingAcksFromSecondaries -= msg.id
        client ! OperationAck(msg.id)
      }
    }
  }


}

