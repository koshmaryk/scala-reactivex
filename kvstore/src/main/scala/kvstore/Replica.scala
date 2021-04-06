package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
import kvstore.Arbiter._
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout
import kvstore.Replica.OperationAck
import kvstore.Replicator.Snapshot

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

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))

  case class SnapshotStatus(replicator: ActorRef, snapshot: Snapshot, persistTimeout: Cancellable)
  case class PersistTimeout(id: Long)

  case class OperationStatus(client: ActorRef,
                             operation: Operation,
                             persisted: Boolean,
                             remainingReplicators: Set[ActorRef],
                             persistTimeout: Cancellable,
                             operationTimeout: Cancellable)
  case class OperationTimeout(id: Long)
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // the currently expected a sequence number
  var expectedSeq = 0L
  // it is the job of the Replica actor to create and appropriately supervise the Persistence actor
  var persistence = context.actorOf(persistenceProps)

  var snapshots = Map.empty[Long, SnapshotStatus]
  var operations = Map.empty[Long, OperationStatus]

  arbiter ! Join

  def receive: Receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Get(key, id) =>
      val valueOption = kv.get(key)
      sender ! GetResult(key, valueOption, id)
    case msg @ Insert(key, value, id) =>
      kv += key -> value
      // threadsafe because scheduler makes sure that the message is delivered after delay to the self and we get it here in the behavior
      val persistTimeout = context.system.scheduler
        .scheduleWithFixedDelay(100.milliseconds, 100.milliseconds, self, PersistTimeout(id))
      val operationTimeout = context.system.scheduler
        .scheduleOnce(1.second, self, OperationTimeout(id))
      val operationStatus = OperationStatus(sender, msg, persisted = false, replicators, persistTimeout, operationTimeout)
      operations += id -> operationStatus
      persistence ! Persist(key, Some(value), id)
      replicators foreach { _ ! Replicate(key, Some(value), id) }
    case msg @ Remove(key, id) =>
      kv -= key
      // threadsafe because scheduler makes sure that the message is delivered after delay to the self and we get it here in the behavior
      val persistTimeout = context.system.scheduler
        .scheduleWithFixedDelay(100.milliseconds, 100.milliseconds, self, PersistTimeout(id))
      val operationTimeout = context.system.scheduler
        .scheduleOnce(1.second, self, OperationTimeout(id))
      val operationStatus = OperationStatus(sender, msg, persisted = false, replicators, persistTimeout, operationTimeout)
      operations += id -> operationStatus
      persistence ! Persist(key, None, id)
      replicators foreach { _ ! Replicate(key, None, id) }
    case PersistTimeout(id) =>
      operations.get(id) match {
        case Some(operationStatus) =>
          val valueOption = operationStatus.operation match {
            case Insert(_, value, _) => Some(value)
            case Remove(_, _) => None
          }
          persistence ! Persist(operationStatus.operation.key, valueOption, operationStatus.operation.id)
        case None =>
      }
    case Persisted(_, id) =>
      operations.get(id) match {
        case Some(operationStatus) =>
          val newOperationStatus = operationStatus.copy(persisted = true)
          if (newOperationStatus.persisted && newOperationStatus.remainingReplicators.isEmpty) {
            operations -= id
            newOperationStatus.operationTimeout.cancel
            newOperationStatus.client ! OperationAck(newOperationStatus.operation.id)
          } else {
            operations += id -> newOperationStatus
            newOperationStatus.persistTimeout.cancel
          }
        case None =>
      }
    case OperationTimeout(id) =>
      operations.get(id) match {
        case Some(operationStatus) =>
          operations -= id
          operationStatus.operationTimeout.cancel
          operationStatus.client ! OperationFailed(id)
        case None =>
      }
    case Replicated(_, id)  =>
      operations.get(id) match {
        case Some(operationStatus) =>
          val newOperationStatus = operationStatus.copy(remainingReplicators = operationStatus.remainingReplicators - sender)
          if (newOperationStatus.persisted && newOperationStatus.remainingReplicators.isEmpty) {
            operations -= id
            newOperationStatus.operationTimeout.cancel
            newOperationStatus.client ! OperationAck(id)
          } else {
            operations += id -> newOperationStatus
          }
        case None =>
      }
    case Replicas(replicas) =>
      val nextReplicas = replicas - self
      val currentReplicas = secondaries.keySet

      val newReplicas = nextReplicas -- currentReplicas
      val droppedReplicas = currentReplicas -- nextReplicas
      val newSecondaries = newReplicas.map { replica =>
        replica -> context.actorOf(Replicator.props(replica))
      }
      val droppedSecondaries = secondaries.view.filterKeys { replica =>
        droppedReplicas.contains(replica)
      }
      val newReplicators = newSecondaries.map { _._2 }
      val droppedReplicators = droppedReplicas.map { secondaries(_) }

      secondaries = secondaries -- droppedSecondaries.keySet ++ newSecondaries
      replicators = replicators -- droppedReplicators ++ newReplicators

      operations.foreach { case (id, op) =>
        val newOperationStatus = op.copy(remainingReplicators = op.remainingReplicators -- droppedReplicators)
        if (newOperationStatus.persisted && newOperationStatus.remainingReplicators.isEmpty) {
          operations -= id
          newOperationStatus.operationTimeout.cancel
          newOperationStatus.client ! OperationAck(id)
        } else {
          operations += id -> newOperationStatus
        }
      }

      for {
        replicator <- newReplicators
        (key, value) <- kv
      }
        replicator ! Replicate(key, Some(value), 0L)

      droppedReplicators.foreach { context.stop }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) =>
      val valueOption = kv.get(key)
      sender ! GetResult(key, valueOption, id)
    case msg @ Snapshot(key, valueOption, seq) =>
      if (seq < expectedSeq) {
        sender ! SnapshotAck(key, seq)
      } else if (seq == expectedSeq) {
        valueOption match {
          case Some(value) => kv += key -> value
          case None =>  kv -= key
        }
        // threadsafe because scheduler makes sure that the message is delivered after delay to the self and we get it here in the behavior
        val timeout = context.system.scheduler
          .scheduleWithFixedDelay(100.milliseconds, 100.milliseconds, self, PersistTimeout(seq))
        snapshots += seq -> SnapshotStatus(sender, msg, timeout)
        persistence ! Persist(key, valueOption, seq)
      }
    case PersistTimeout(seq) => // for secondaries as id used seq
      snapshots.get(seq) match {
        case Some(snapshotStatus) => persistence ! Persist(snapshotStatus.snapshot.key, snapshotStatus.snapshot.valueOption, snapshotStatus.snapshot.seq)
        case None =>
      }
    case Persisted(_, seq) => // for secondaries as id used seq
      snapshots.get(seq) match {
        case Some(snapshotStatus) =>
          snapshots -= seq
          snapshotStatus.persistTimeout.cancel
          snapshotStatus.replicator ! SnapshotAck(snapshotStatus.snapshot.key, snapshotStatus.snapshot.seq)
          expectedSeq += 1L
        case None =>
      }
  }
}
