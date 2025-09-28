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
use "time"

use @printf[I32](fmt: Pointer[U8] tag, ...)
use @fprintf[I32](stream:Pointer[U8], fmt: Pointer[U8] tag, ...)
use @exit[None](status:I32)
use @pony_os_stdout[Pointer[U8]]()
use @pony_os_stderr[Pointer[U8]]()
use @fflush[I32](stream:Pointer[U8])
use @write[USize](fd:I32, buf:Pointer[U8] tag, sz:USize)

primitive Assert
  fun crash(msg:String, loc:SourceLoc = __loc) =>
      // try to get our own line for better visibility.
      @fflush[I32](@pony_os_stdout())
      @fflush[I32](@pony_os_stderr())
      let msg3 = "\n"+loc.file() + ":" + loc.line().string() + "\n" + loc.type_name() + "." + loc.method_name() + ": " + msg + "\n"
      @write(I32(2), msg3.cstring(), msg3.size())  // 2 = stderr
      @fflush[I32](@pony_os_stderr())
      @exit(1)
    
  fun equal[T: (Equatable[T] #read & Stringable #read)](got:T, want:T, loc: SourceLoc = __loc) =>
    if got != want then
      crash("error: Assert.equal violated! want: " + want.string() + ", but got: " + got.string(), loc)
    end 

  fun equalbox[T: (Equatable[T] box & Stringable box)](got:T, want:T, loc: SourceLoc = __loc) =>
    if got != want then
      crash("error: Assert.equal violated! want: " + want.string() + ", but got: " + got.string(), loc)
    end 
    
  fun lte[T: (Comparable[T] #read & Stringable #read)](a:T, b:T, inv:String, loc: SourceLoc = __loc) =>
    if a <= b then
      return
    end
    crash("error: Assert.lte violated! want: " + a.string() + " <= " + b.string() + " since '" + inv + "'", loc)

  fun gte[T: (Comparable[T] #read & Stringable #read)](a:T, b:T, inv:String, loc: SourceLoc = __loc) =>
    if a >= b then
      return
    end
    crash("error: Assert.gte violated! want: " + a.string() + " <= " + b.string() + " since '" + inv + "'", loc)

  fun lt[T: (Comparable[T] #read & Stringable #read)](a:T, b:T, inv:String, loc: SourceLoc = __loc) =>
    if a < b then
      return
    end
    crash("error: Assert.lt violated! want: " + a.string() + " < " + b.string() + " since '" + inv + "'")

  fun gt[T: (Comparable[T] #read & Stringable #read)](a:T, b:T, inv:String, loc: SourceLoc = __loc) =>
    if a > b then
      return
    end
    crash("error: Assert.gt violated! want: " + a.string() + " > " + b.string() + " since '" + inv + "'", loc)
    
    
  fun invar(mustHold:Bool, invariantText:String, loc: SourceLoc = __loc) =>
    if mustHold then
      return
    end
    crash("error: Assert.invar '" + invariantText + "' violated!", loc)
    

actor MainFifo // acts as our test parent in _TestInfoBasic, for the *Done behaviors.
  let _out: OutStream
  let _h:TestHelper
  
    new create(h:TestHelper) =>
      _out = h.env.out
      _h = h
        
    be producerDone(h:TestHelper) =>
        h.complete(true) // success, stop the long_test timeout
      
    be consumerDone(h:TestHelper, t0: (I64,I64)) =>
        let t1 = Time.now()
        let elap = (t1._2 - t0._2) + ((t1._1 - t0._1) * 1_000_000_000)
        _out.print("elapsed nanosec = " + elap.string())    
        h.complete(true) // success, stop the long_test timeout

// class val Product
//     let id: I64
//     new create(id':I64) =>
//         id = id'
//     fun string(): String =>
//         id.string()

type Product is I64        


actor Fifo
    let _out: OutStream
    let _cap: USize
    //let _buf: Array[Product iso]
    let _buf: Array[Product val]
    var _promised: I64 = 0
    //var _isClosed: Bool = false
    let _h:TestHelper
    let _env:Env

    var _waitQprod: Array[(Producer,I64)] = Array[(Producer,I64)]
    var _waitQcons: Array[(Consumer,I64)] = Array[(Consumer,I64)]

    // treat _buf like a ring buffer so we don't
    // have to do so much try / end error handling.
    var _ringBeg: USize = 0
    var _ringReadable: USize = 0 // replace .size() with this.

    fun ref popfront(): Product val? =>
      _ringReadable = _ringReadable -1
      let front = _buf(_ringBeg)?
      _ringBeg = _ringBeg + 1
      if _ringBeg == _cap then
        _ringBeg = 0
      end
      front

    fun ref pushback(p: Product val)? =>
        let writeStart = (_ringBeg + _ringReadable) % _cap
        _buf(writeStart)? = p
        _ringReadable = _ringReadable + 1
    
    fun ref _clearQs() =>
        _waitQprod = Array[(Producer,I64)]
        _waitQcons = Array[(Consumer,I64)]

    new create(out:OutStream, n:USize, h:TestHelper) =>
        _h = h
        _cap = n
        _out = out
        //_buf = Array[Product iso].init(n.usize()) // set capacity. use if Product is class.
        _buf = Array[Product val].init(0, n.usize()) // set capacity. use if Product is I64 type
        //_out.print("fifo: created with capacity: " + n.string() + " and size: " + _buf.size().string())
        _env = h.env

    be close() =>
        """
        close releases all internal references, allowing
        them to be garbage collected.
        Any future messages (behavior calls) will be ignored.
        """
        //if _isClosed then
        //    return
        //end
        _clearQs()
        _buf.clear() //  = Array[Product iso](0)
        //_isClosed = true

    be consumerCancel(fromCons:Consumer, next:I64) =>
        """
        consumerCancel tells the Fifo that fromCons is
        no longer interested in consuming. This cancels
        the effects of any prior Fifo.consumerRequestNext() call.
        This is a safe no-op if fromCons is
        not already waiting at the fifo.
        """
        //if _isClosed then
        //    return
        //end
        if _waitQcons.size() == 0 then
            return
        end
        var i:USize = 0
        for c in _waitQcons.values() do
            (let cc, let nextcc) = c
            if nextcc != next then
                // bark if stored nextcc != call next
                None                
            end
            if cc is fromCons then
                //_out.print("fifo: consumerCancel removing fromCons from _waitQcons")
                try _waitQcons.delete(i)? end
                fromCons.ackCancel(next)
                break
            end
            i=i+1
        end

    be consumerRequestsNext(fromCons:Consumer, next:I64) =>
        //if _isClosed then
        //    return
        //end
        if _ringReadable == 0 then
            //_out.print("fifo: consumerRequestsNext: nothing for consumer, add them to _waitQcons")
            _waitQcons.push((fromCons, next))
            return
        end
        //Assert.lte[I64](_buf.size().i64(), _cap, "buf must be <= _cap")
        //_out.print("fifo: consumerRequestsNext() has _buf.size = " + _buf.size().string())
       
        try
            var x = popfront()?          
            //var x = _buf.delete(0)?
            //_out.print("fifo: consumerRequestsNext about to provide = " + x.string() + " ; now _buf.size = " + _buf.size().string())
            // assert we get the expected next
            //let x_id = id
            //let x_id  = x.i64()
            //Assert.equal[I64](x, next)
            //Assert.equalbox[Product box](x, next)
            //if x != next then
            //  Assert.crash("x != next")
            //end
            
            fromCons.consumeThis(consume x)
        end
        nudgeProducer()

    //be append(producer:Producer, product:Product iso) => // class Product
    be append(producer:Producer, product:Product val) =>   // type Product is I64
      //if _isClosed then
      //  return
      //end
      //Assert.gt[I64](_promised, 0, "_promised must be > 0 in append(); product = " + product.string()) // fires!
      //Assert.lt[I64](_buf.size().i64(), _cap, "buf must be < _cap before a push; promised = " + _promised.string())

      try
          pushback(consume product)?      
          //_buf.push(consume product)
          _promised = _promised - 1
          dispatch()
      end

    fun ref dispatch() =>
        if (_ringReadable == 0) or (_waitQcons.size() == 0) then    
            return // nothing to deliver, or no consumer to deliver it to.
        end

        try
            (var fromCons, var next) = _waitQcons.delete(0)?
            //_out.print("fifo: dispatch() is taking consumer off _waitQcons to provide them next = "+next.string())
            //fromCons.consumeThis(_buf.delete(0)?)
            fromCons.consumeThis(popfront()?)            
        end
        nudgeProducer()

    fun ref nudgeProducer() =>
        if (_waitQprod.size() > 0) and ((_ringReadable.i64() + _promised) < _cap.i64()) then
            // the producer can now use the newly freed slot
            try
                (let producer, let next) = _waitQprod.delete(0)?
                //_out.print("fifo: sees free slot and waiting producer, allowing = " + next.string())
                _promised = _promised + 1
                producer.produce(next)
            end
        end


    be requestToProduce(fromProd:Producer, next:I64) =>
        //if _isClosed then
        //    return
        //end
        if (_ringReadable.i64() + _promised) < _cap.i64() then
            //_out.print("fifo: requestToProduce(next="+next.string()+") allows producer to produce id = " + next.string()+ " since _buf.size = " + _buf.size().string() + " and _promised = " + _promised.string() + " together are < _cap == " + _cap.string())
            _promised = _promised + 1
            fromProd.produce(next)
        else
            //_out.print("fifo: requestToProduce() adds producer to waitQ at next = " + next.string())
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
        //if _isClosed then
        //    return
        //end
        if _waitQprod.size() == 0 then
            return
        end
        var i:USize = 0
        for c in _waitQprod.values() do
            (let cc, _) = c // should we bark if stored next != call next?
            if cc is fromProd then
                //_out.print("fifo: cancelReqToProduce removing fromProd from _waitQprod")
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
    var _saw: I64 = -1 // assert we see contiguous integers
    let _t0: (I64,I64)    

    new create(out:OutStream, n:I64, fifo:Fifo, parent:MainFifo, h: TestHelper, t0:(I64,I64)) =>
        //out.print("consumer: created. will consume " + n.string())
        _fifo = fifo
        _out = out
        _n = n
        _parent = parent
        _h = h
        _t0 = t0
        
    be start() =>
        //_out.print("consumer: started. _next = " + _next.string())
        _fifo.consumerRequestsNext(this, _next)

    //be consumeThis(prod: Product iso) => // class Product
  be consumeThis(prod: Product val) =>   // type Product is I64
    //Assert.equal[I64](prod, _saw+1) // consume must happen in order so prod must == _saw+1
    _saw = _saw + 1
    //_out.print("consumer: has consumed " + prod.string())
    _next = _next + 1 // should == prod.id + 1
    if _next < _n then
      //_out.print("consumer: about to ask for _next = " + _next.string())
      _fifo.consumerRequestsNext(this, _next)
    else
      // notify parent
      _parent.consumerDone(_h, _t0)
    end
          
  be ackCancel(next:I64) => None
        //_out.print("consumer: ackCancel for " + next.string())


actor Producer
    let _out: OutStream
    let _n: I64
    let _fifo: Fifo
    var _next: I64 = 0
    let _parent: MainFifo
    let _h: TestHelper

    new create(out:OutStream, n:I64, fifo:Fifo, parent:MainFifo, h: TestHelper) =>
        _out = out
        //_out.print("producer: created. will produce " + n.string())
        _fifo = fifo
        _n = n
        _parent = parent
        _h = h

    be start() =>
        //_out.print("producer: started. _next = " + _next.string())
        _fifo.requestToProduce(this, _next)

  be produce(id:I64) =>
        //let pr:Product = id
        //let newProduct:Product iso = recover id end // Product(id))
        //let newProduct:Product iso = Product(id)
        //_fifo.append(this, consume newProduct) 
        _fifo.append(this, Product(id)) 
        //_out.print("producer: has produced " + id.string())
        _next = _next + 1
        if _next < _n then
                _fifo.requestToProduce(this, _next)
        else
            // notify parent
            _parent.producerDone(_h)
        end

    be ackCancel(next:I64) => None
        //_out.print("producer: ackCancel for " + next.string())

