use "pony_test"
use "time"
    

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

