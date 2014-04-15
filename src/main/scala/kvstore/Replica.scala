package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout
import scala.Some
import kvstore.Arbiter.Replicas
import scala.collection.SortedSet
import scala.util.Try
//import akka.event.LoggingReceive

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

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props[Replica](new Replica(arbiter, persistenceProps))

  val persistenceInterval = 50 millis span
  val messageFailureTimeout = 1 second span
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]

  var replicators = Set.empty[ActorRef]  
  var secondaries = Map.empty[ActorRef, ActorRef]
  


  val persistence: ActorRef = context.actorOf(persistenceProps, "persistence")

  var snapshotSequenceId: Long = 0
  
  var persLeaderIds = Map.empty[Long, Cancellable]
  var persReplicaIds = Map.empty[Long, (ActorRef, Cancellable)]


  var opLeaderAcknowledgements = Map.empty[Long, (ActorRef, Set[ActorRef], Boolean, Cancellable)]
  var replicaIdSet = Set.empty[Long]
  var maxReplicaId = 0L

  override def preStart() {
    arbiter ! Join
    context.watch(persistence)
  }

  override def postStop() {
    persLeaderIds.values foreach {
      case persistenceScheduler => persistenceScheduler.cancel()
    }
    persReplicaIds.values foreach {
      case (_, persistenceScheduler) => persistenceScheduler.cancel()
    }
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for the leader role. */
  val leader: Receive = {
    
    
    case Insert(key, value, id) =>
      
      kv += key -> value
      
      replicators foreach { _ ! Replicate(key, Some(value), id) }
      
      val persistMessage = Persist(key, Some(value), id)
      val failureMessage = OperationFailed(id)
      
      val persistenceScheduler = context.system.scheduler.schedule(Duration.Zero, persistenceInterval, persistence, persistMessage)
      
      val failureScheduler = context.system.scheduler.scheduleOnce(messageFailureTimeout, sender, failureMessage)
      
      persLeaderIds.get(id).foreach{ _.cancel() }
      
      persLeaderIds += id -> persistenceScheduler
      
      opLeaderAcknowledgements += id -> (sender, replicators, false, failureScheduler)
      
      
    case Remove(key, id) =>
      
      kv -= key
      replicators.foreach { _ ! Replicate(key, None, id) }
      val persistMessage = Persist(key, None, id)
      val failureMessage = OperationFailed(id)

      val persistenceScheduler = context.system.scheduler.schedule(Duration.Zero, persistenceInterval, persistence, persistMessage)
      val failureScheduler = context.system.scheduler.scheduleOnce(messageFailureTimeout, sender, failureMessage)
      persLeaderIds.get(id).foreach { _.cancel() }
      
      persLeaderIds += id -> persistenceScheduler
      
      opLeaderAcknowledgements.get(id).foreach { case (_, _, _, interval) => interval.cancel() }
      
      opLeaderAcknowledgements += id -> (sender, replicators, false, failureScheduler)
      
    
    
    case Replicas(replicas) =>
      // add new replicas
      
      var maxId = Try(replicaIdSet.max ).getOrElse( maxReplicaId ) + 1
      
      
      replicas.filterNot( secondaries.contains ).filter { _ != self } foreach { newReplica =>
        // create a replica
         val newReplicator = context.actorOf(Replicator.props(newReplica))
         
         secondaries += newReplica -> newReplicator
         replicators += newReplicator
         
         kv foreach { case (key, value) =>
           newReplicator ! Replicate(key, Some(value), maxId)
           replicaIdSet += maxId
           maxReplicaId = maxId
           maxId += 1
           
         }
         
         
      }
      
      
      
      // remove old replicas
      secondaries.keys.filterNot( replicas.contains ).foreach { removedReplica =>
        secondaries(removedReplica) ! PoisonPill
        secondaries -= removedReplica
      }
      
    case Persisted(key, id) =>
      persLeaderIds.get(id).foreach{ scheduler =>
          scheduler.cancel()
          persLeaderIds -= id
          
          opLeaderAcknowledgements.get(id).foreach { 
            case (theSender, myReplicators, false, failureScheduler) =>
              if (myReplicators.isEmpty) {
                
                if (failureScheduler.cancel()) {
                  theSender ! OperationAck(id)
                }
                
                opLeaderAcknowledgements -= id
              } else {
                opLeaderAcknowledgements += id -> (theSender, myReplicators, true, failureScheduler)
              }
            case _ =>
          }
      }
      
      
    case Replicated(key, id) =>
      
      opLeaderAcknowledgements.get(id) match {
        case Some((theSender, myReplicators, persisted, failureScheduler)) =>
          val newReplicatorsSet = myReplicators - sender
          
          if (persisted && newReplicatorsSet.isEmpty ) {
            opLeaderAcknowledgements -= id
            if (failureScheduler.cancel()) {
              theSender ! OperationAck(id)
            }
            
          } else {
            opLeaderAcknowledgements += id -> (theSender, newReplicatorsSet, persisted, failureScheduler)
          }
        case None =>
          if (replicaIdSet.contains(id)) {
            replicaIdSet -= id
          } 
      }
      
      case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
  }

  
  // replica behavior
  val replica: Receive = {
    
    
    
    case Snapshot(key, valueOption, seq) =>
      
      if (seq < snapshotSequenceId) {
        
        sender ! SnapshotAck(key, seq)
        
      } else if (seq == snapshotSequenceId) {
        valueOption match {
          case Some(value) => kv += key -> value
          case None => kv -= key
        }

        
        val persistenceScheduler = context.system.scheduler.schedule(Duration.Zero, persistenceInterval, persistence, Persist(key, valueOption, seq))
        
        persReplicaIds.get(seq).foreach { case (_, ps) => ps.cancel() }
        
        persReplicaIds += seq -> (sender, persistenceScheduler)
        
        snapshotSequenceId = seq + 1
        
      }
      
      
    case Persisted(key, seq) =>
        persReplicaIds.get(seq).foreach { case (theSender, persistenceScheduler) =>
            persistenceScheduler.cancel()
            persReplicaIds -= seq
            theSender ! SnapshotAck(key, seq)

        }
        
        
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)    
  }
  
  
  
  

}