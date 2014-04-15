package kvstore

import akka.actor._
import scala.concurrent.duration._
import scala.Some
//import akka.event.LoggingReceive

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props[Replicator](new Replicator(replica))

  val snapshotSendTimeout = 100.milliseconds
}

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  
  var acks = Map.empty[Long, (ActorRef, String, Long, Cancellable)]
  
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Map.empty[String, Long]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  override def preStart() {
    context.watch(replica)
  }
  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case Replicate(key, valueOption, id) =>  
      
	    replicateToAll(key)
	      
	    val nextInSeq = nextSeq
	
	    val snapshotScheduler = context.system.scheduler.schedule(Duration.Zero, snapshotSendTimeout, replica, Snapshot(key, valueOption, nextInSeq))
	    
	    acks += nextInSeq -> (sender, key, id, snapshotScheduler)
	    
	    pending += key -> nextInSeq
	
	    case SnapshotAck(key, seq) =>
	      acks.get(seq).foreach { case (sourceActor, newKey, id, snapshotScheduler) =>
	
	          pending.get(key).foreach{ seqNum => 
	            if( seqNum <= seq ){
	              pending -= key
	            }
	          }
	        
	          acks -= seq
	
	          snapshotScheduler.cancel()
	          sourceActor ! Replicated(newKey, id)          
	      }

  }

  override def postStop() {
    acks.values.foreach { case (theSender, theKey, id, snapshotScheduler) =>
        snapshotScheduler.cancel()

        theSender ! Replicated(theKey, id)
    }
  }
  
  private def replicateToAll(key: String): Unit = {
    
    pending.get(key).foreach { seq =>
        acks.get(seq).foreach { case (t, k, i, scheduler) =>
            acks -= seq
            pending -= key
            scheduler.cancel()
            
            
            t ! Replicated(k, i)
        }
    }
  }

}