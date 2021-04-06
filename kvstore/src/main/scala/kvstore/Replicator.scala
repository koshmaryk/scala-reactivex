package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, Props}

import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  case class ReplicateStatus(primary: ActorRef, replicate: Replicate, timeout: Cancellable)
  case class SnapshotTimeout(seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, ReplicateStatus]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L
  def nextSeq(): Long = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }


  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case msg @ Replicate(key, valueOption, _) =>
      val seq = nextSeq()
      // threadsafe because scheduler makes sure that the message is delivered after delay to the self and we get it here in the behavior
      val timeout = context.system.scheduler
        .scheduleWithFixedDelay(100.milliseconds, 100.milliseconds, self, SnapshotTimeout(seq))
      val status = ReplicateStatus(sender, msg, timeout)
      acks += seq -> status
      replica ! Snapshot(key, valueOption, seq)
    case SnapshotTimeout(seq) =>
      acks.get(seq) match {
        case Some(replicateStatus) => replica ! Snapshot(replicateStatus.replicate.key, replicateStatus.replicate.valueOption, seq)
        case None =>
      }
    case SnapshotAck(key, seq) =>
      acks.get(seq) match {
        case Some(replicateStatus) =>
          acks -= seq
          replicateStatus.timeout.cancel
          replicateStatus.primary ! Replicated(key, replicateStatus.replicate.id)
        case None =>
      }
  }
}
