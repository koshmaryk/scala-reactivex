/**
  * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
  */
package actorbintree

import akka.actor._

import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `req` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `req` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `req` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply
}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive: Receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case msg: Operation => root ! msg
    case GC =>
      val newRoot = createRoot
      pendingQueue = Queue.empty[Operation]
      context.become(garbageCollecting(newRoot))
      root ! CopyTo(newRoot)
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case msg: Operation => pendingQueue = pendingQueue.enqueue(msg)
    case GC =>
    case CopyFinished =>
      root = newRoot
      context.become(normal)
      pendingQueue.foreach { newRoot ! _ }
  }
}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
    * Acknowledges that a copy has been completed. This message should be sent
    * from a node to its parent, when this node and all its children nodes have
    * finished being copied.
    */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean): Props = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean = false) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive: Receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case msg @ Contains(req, id, curr) =>
      if (elem == curr) {
        req ! ContainsResult(id, !removed)
      } else {
        val position = if (curr < elem) Left else Right
        subtrees.get(position) match {
          case Some(tree) => tree ! msg
          case None => req ! ContainsResult(id, result = false)
        }
      }
    case msg @ Insert(req, id, curr) =>
      if (elem == curr) {
        removed = false
        req ! OperationFinished(id)
      } else {
        val position = if (curr < elem) Left else Right
        subtrees.get(position) match {
          case Some(tree) => tree ! msg
          case None =>
            subtrees = subtrees.updated(position, context.actorOf(props(curr, initiallyRemoved = false)))
            req ! OperationFinished(id)
        }
      }
    case msg @ Remove(req, id, reqElem) =>
      if (reqElem == elem) {
        removed = true
        req ! OperationFinished(id)
      } else {
        val position = if (reqElem > elem) Right else Left
        subtrees.get(position) match {
          case Some(subtree) => subtree ! msg
          case None => req ! OperationFinished(id)
        }
      }
    case msg @ CopyTo(treeNode) =>
      val children = subtrees.values.toSet
      if (removed && children.isEmpty) {
        context.parent ! CopyFinished
        context.stop(self)
      } else {
        context.become(copying(children, removed))
        if (!removed) {
          treeNode ! Insert(self, 0, elem)
        }
        children foreach {
          _ ! msg
        }
      }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(_) =>
      if (expected.isEmpty) {
        context.parent ! CopyFinished
        context.stop(self)
      } else {
        context.become(copying(expected, insertConfirmed = true))
      }
      
    case CopyFinished =>
      val newExpected = expected - sender
      if (newExpected.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
        context.stop(self)
      } else {
        context.become(copying(newExpected, insertConfirmed = true))
      }
  }
}
