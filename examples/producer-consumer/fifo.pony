//
// A classic producer/consumer synchronization problem.
//
// According to https://en.wikipedia.org/wiki/Producer-consumer_problem
// the family of producer/consumer problems was first described
// and solved by Dijkstra in 1965.
//
// https://www.cs.utexas.edu/users/EWD/transcriptions/EWD01xx/EWD123.html#4.1.%20Typical%20Uses%20of%20the%20General%20Semaphore.
// 
// In the general problem, there is a channel or buffer between
// the producer and consumer. The purpose of the buffer is
// to allow either end to operate at different speeds
// without causing excessive delay to the other side,
// and without losing information. If the buffer is finite,
// it offers flow control, in that it guards too against having
// the producer overwhelm the consumer with too much
// information at once. The buffer is assumed to be
// much faster to read or write than the producing
// and consuming processes, so it "smooths out"
// the rate of data transfer, avoiding large spikes and
// pauses.
//
// In problem varitations, there can be different numbers
// of producers and consumers, and the channel can
// be finite or infinite. These can acheive various fairness
// or lowest average wait-time properties.
//
// Here we will define the problem to be solved as
// using a finite channel of a given size. We will
// require only a single producer and a single consumer.
//
// Problem definition:
//
// We are given a single producer and a single consumer,
// who wish to transfer data "products" in one direction, via
// an intermediate channel (aka buffer). While the data
// payloads flow in one direction only, control information may
// flow in both directions. We picture the data flow:
//
//       producer -> channel -> consumer
//
// Continuing the manufacturing metaphor, we say that
// the producer may only fill _empty_ slots in the channel.
// The consumer may only remove and consume products that
// are currently occupying a _full_ slot in the channel.
//
// Our specific problem to solve here asks that our channel have
// a fixed capacity: a given number of slots for product. 
// Our solution must convey product in a first-in-first-out
// (fifo) manner.
//
// The single producer writes to the channel to produce
// a product, and this may succeed so long as there is available space
// in the channel. If not, the producer must wait for the consumer
// to consume some from the channel, before producing more.
//
// The single consumer reads/removes products from a non-empty fifo
// channel when they wish. They receive or read
// products from the fifo in the same order they were written
// by the producer.
//
// The consumer should never be able to consume more
// than has been produced, and it should never consume
// the same item twice.
//
// When the channel is full, the producer must wait
// until the consumer has retrieved
// product and thus freed up space.
//
// Nothing written by the producer should be later
// overwritten and lost by further production before
// the consumer has consumed it.
//
// The sequence of consumption should match the production order,
// reflecting the FIFO's guarantee of a first-in, first-out
// discipline.
//
// Consuming a product from the fifo frees
// up space in the fifo for the producer to continue producing, if
// they wish. Either end can cease or resume their operations
// at any point. They are not allowed to overflow or
// underflow the channel. There cannot be a negative product
// count in the channel, and the data count cannot exceed the
// channel's fixed capacity.
//
// cancellation
// ------------
//
// The consumer who wants to consume and finds an
// empty queue must be prepared to wait a while
// for a response; having their request queued until
// product arrives. We said above the consumer can
// cease at any time. However, once they have asked
// to consume an item, this intent may well have been registered
// at the FIFO. If the consumer were to cease and
// just walk away without again informing the
// fifo of their intent to depart, the fifo might
// later send confusing traffic (fifo:"here is that product
// you ordered!"; consumer:"what? what order? I didn't
// place any orders _recently_!"). A queued consumer may
// even keep alive a consumer who could otherwise be
// garbage collected, which could waste memory.
//
// Thus cancelation of a consume request should be
// a feature. When expanding to multiple consumers,
// the cancellation feature will allow
// the fifo in future solutions to gracefully share
// product only to other consumers who do wish to
// receive it.
//
// Hence we seek to treat the intent-to-consume as 
// the start of an atomic transaction. In proper
// usage, the consume action must either fully complete,
// by receiving a callback, or fully abort by telling
// the fifo to cancel the prior request. This
// avoids wasting time spent on sending data
// to a consumer who is no longer interested.
// Note that due the logical race involved in
// cancellation, even after a cancel call the
// consumer could still receive a consume callback.
// Thus the consumer should wait for acknowledgement
// of their cancellation if they want to
// avoid such confusing future messages.
//
// (Aside: A similar rationale dictated the TCP
// two-roundtrip 4-state shutdown sequence
// and the dreaded TIME_WAIT status.)
//
// Symmetrically, producers who have registered
// intent to produce should be able to
// cancel that at the fifo and get acknowledgement.
//
// That is the problem statement.
//
// Here is one solution:

use "pony_test"

actor MainFifo // acts as our test parent in _TestInfoBasic, for the *Done behaviors.
  let _out: OutStream
  new create(env:Env) =>
     _out = env.out

  be producerDone(h:TestHelper) =>
      h.complete(true) // success, stop the long_test timeout  
      
  be consumerDone(h:TestHelper) =>
      h.complete(true) // success, stop the long_test timeout  

class val Product
  let id: I64
  new create(id':I64) =>
    id = id'
  fun string(): String =>
     id.string()

actor Fifo
  let _out: OutStream
  let _cap: I64
  var _buf: Array[Product iso]
  var _promised: I64 = 0
  var _isClosed: Bool = false

  var _waitQprod: Array[(Producer,I64)] = Array[(Producer,I64)]
  var _waitQcons: Array[(Consumer,I64)] = Array[(Consumer,I64)]

  fun ref _clearQs() =>
     _waitQprod = Array[(Producer,I64)]
     _waitQcons = Array[(Consumer,I64)]

  new create(out:OutStream, n:I64) =>
     _cap = n
     _out = out
     _buf = Array[Product iso](n.usize()) // set capacity
     _out.print("fifo: created with capacity: " + n.string() + " and size: " + _buf.size().string())

  be close() =>
      """
      close releases all internal references, allowing
      them to be garbage collected.
      Any future messages (behavior calls) will be ignored.
      """
      if _isClosed then
         return
      end
      _clearQs()
      _buf = Array[Product iso](0)
      _isClosed = true

  be consumerCancel(fromCons:Consumer, next:I64) =>
       """
       consumerCancel tells the Fifo that fromCons is
       no longer interested in consuming. This cancels
       the effects of any prior Fifo.consumerRequestNext() call.
       This is a safe no-op if fromCons is
       not already waiting at the fifo.
       """
       if _isClosed then
          return
       end       
       if _waitQcons.size() == 0 then
         return
       end
       var i:USize = 0
       for c in _waitQcons.values() do
           (let cc, _) = c // should we bark if stored next != call next?
           if cc is fromCons then
              _out.print("fifo: consumerCancel removing fromCons from _waitQcons")
              try _waitQcons.delete(i)? end
              fromCons.ackCancel(next)
              break
           end
           i=i+1
       end

  be consumerRequestsNext(fromCons:Consumer, next:I64) =>
       if _isClosed then
          return
       end
       if _buf.size() == 0 then
           _out.print("fifo: consumerRequestsNext: nothing for consumer, add them to _waitQcons")
           _waitQcons.push((fromCons, next))
           return
       end
       _out.print("fifo: consumerRequestsNext() has _buf.size = " + _buf.size().string())
       
       try
          var x = _buf.delete(0)?
          _out.print("fifo: consumerRequestsNext about to provide = " + x.string() + " ; now _buf.size = " + _buf.size().string())
          fromCons.consumeThis(consume x)
       end
       nudgeProducer()

  be append(producer:Producer, product:Product iso) =>
       if _isClosed then
          return
       end  
       _buf.push(consume product)
       _promised = _promised - 1
       dispatch()

  fun ref dispatch() =>
       if (_buf.size() == 0) or (_waitQcons.size() == 0) then
         return // nothing to deliver, or no consumer to deliver it to.
       end

       try
          (var fromCons, var next) = _waitQcons.delete(0)?
          _out.print("fifo: dispatch() is taking consumer off _waitQcons to provide them next = "+next.string())
          fromCons.consumeThis(_buf.delete(0)?)
       end
       nudgeProducer()

  fun ref nudgeProducer() =>
       if (_waitQprod.size() > 0) and ((_buf.size().i64() + _promised) < _cap) then
          // the producer can now use the newly freed slot
          try
             (let producer, let next) = _waitQprod.delete(0)?
             _out.print("fifo: sees free slot and waiting producer, allowing = " + next.string())
             producer.produce(next)
          end
       end


  be requestToProduce(fromProd:Producer, next:I64) =>
        if _isClosed then
           return
        end
        if (_buf.size().i64() + _promised) < _cap then
           _out.print("fifo: requestToProduce() allows producer to produce id = " + next.string())
           _promised = _promised + 1
           fromProd.produce(next)
        else
           _out.print("fifo: requestToProduce() adds producer to waitQ at next = " + next.string())
           _waitQprod.push((fromProd, next))
        end

  be cancelReqToProduce(fromProd:Producer, next:I64) =>
       """
       cacnelReqToProduce tells the Fifo that fromProd is
       no longer interested in producing. This cancels
       the effects of any prior Fifo.requestToProduce() call.
       This is a safe no-op if fromProd is
       not already waiting at the fifo.
       """
       if _isClosed then
          return
       end
       if _waitQprod.size() == 0 then
         return
       end
       var i:USize = 0
       for c in _waitQprod.values() do
           (let cc, _) = c // should we bark if stored next != call next?
           if cc is fromProd then
              _out.print("fifo: cancelReqToProduce removing fromProd from _waitQprod")
              try _waitQprod.delete(i)? end
              fromProd.ackCancel(next)
              break
           end
           i=i+1
       end
  

actor Consumer
  let _out: OutStream
  let _n: I64
  var _next: I64 = 0
  let _fifo: Fifo
  let _parent: MainFifo
  let _h: TestHelper

  new create(out:OutStream, n:I64, fifo:Fifo, parent:MainFifo, h: TestHelper) =>
     out.print("consumer: created. will consume " + n.string())
     _fifo = fifo
     _out = out
     _n = n
     _parent = parent
     _h = h

  be start() => 
     _out.print("consumer: started. _next = " + _next.string())
     _fifo.consumerRequestsNext(this, _next)

  be consumeThis(prod: Product iso) =>
     _out.print("consumer: has consumed " + prod.string())
     _next = _next + 1 // should == prod.id + 1 
     if _next < _n then
        _out.print("consumer: about to ask for _next = " + _next.string())
        _fifo.consumerRequestsNext(this, _next)
     else
        // notify parent
        _parent.consumerDone(_h)     
     end

  be ackCancel(next:I64) =>
     _out.print("consumer: ackCancel for " + next.string())


actor Producer
  let _out: OutStream
  let _n: I64
  let _fifo: Fifo
  var _next: I64 = 0
  let _parent: MainFifo
  let _h: TestHelper

  new create(out:OutStream, n:I64, fifo:Fifo, parent:MainFifo, h: TestHelper) =>
     out.print("producer: created. will produce " + n.string())
     _fifo = fifo
     _out = out
     _n = n
     _parent = parent
     _h = h

  be start() =>
     _out.print("producer: started. _next = " + _next.string())
     _fifo.requestToProduce(this, _next)

  be produce(id:I64) =>
     _fifo.append(this, Product(id))
     _out.print("producer: has produced " + id.string())
     _next = _next + 1
     if _next < _n then
        _fifo.requestToProduce(this, _next)
     else
        // notify parent
        _parent.producerDone(_h)
     end

  be ackCancel(next:I64) =>
     _out.print("producer: ackCancel for " + next.string())
