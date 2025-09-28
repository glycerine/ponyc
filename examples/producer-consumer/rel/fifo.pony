use "time"

actor Main
  let _out: OutStream
  new create(env:Env) =>
    _out = env.out

  let t0 = Time.now()
  let numToProduce: I64 = 20
  let numToConsume: I64 = numToProduce
  let fifoSize: USize = 2
  let fifo = Fifo(_out, fifoSize)

  let p = Producer(_out, numToProduce, fifo, this)
  let c = Consumer(_out, numToConsume, fifo, this, t0)

  c.start()
  p.start()


  be consumerDone(t0: (I64,I64)) =>
    let t1 = Time.now()
    let elap = (t1._2 - t0._2) + ((t1._1 - t0._1) * 1_000_000_000)
    _out.print("elapsed nanosec = " + elap.string())

type Product is I64
   
actor Fifo
  let _out: OutStream
  let _cap: USize
  var _promised: I64 = 0

  var _buf: Array[Product val]

  // treat _buf like a ring buffer so we don't
  // have to do so much try / end error handling.
  
  var _ringBeg: USize = 0
  var _ringReadable: USize = 0 // replace .size() with this.

  var _waitQprod: Array[(Producer,I64)] = Array[(Producer,I64)]
  var _waitQcons: Array[(Consumer,I64)] = Array[(Consumer,I64)]

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

  new create(out:OutStream, n:USize) =>
    _cap = n
    _out = out
    _buf = Array[Product val].init(0, n) // allocate all n for ring

  be consumerRequestsNext(fromCons:Consumer, next:I64) =>
    if _ringReadable == 0 then
      _waitQcons.push((fromCons, next))
      return
    end
     
    try
      //var x = _buf.delete(0)?
      var x = popfront()?
      fromCons.consumeThis(consume x)
    end
    nudgeProducer()

  be append(producer:Producer, product:Product val) =>
    _promised = _promised - 1
  
    // can we bypass the _buf and go directly
    // to a waiting consumer? yeah. but that measured slower(!)
    try
      pushback(consume product)?
      dispatch()
    end

  fun ref dispatch() =>
    if (_ringReadable == 0) or (_waitQcons.size() == 0) then
      return // nothing to deliver, or no consumer to deliver it to.
    end

    try
      (var fromCons, var next) = _waitQcons.delete(0)?
      
      //fromCons.consumeThis(_buf.delete(0)?)
      fromCons.consumeThis(popfront()?)
    end
    nudgeProducer()

  fun ref nudgeProducer() =>
    if (_waitQprod.size() > 0) and ((_ringReadable.i64() + _promised) < _cap.i64()) then
      // the producer can now use the newly freed slot
      try
        (let producer, let next) = _waitQprod.delete(0)?
        _promised = _promised + 1        
        producer.produce(next)
      end
    end


  be requestToProduce(fromProd:Producer, next:I64) =>
    if (_ringReadable.i64() + _promised) < _cap.i64() then
      _promised = _promised + 1
      fromProd.produce(next)
    else
      _waitQprod.push((fromProd, next))
    end

  

actor Consumer
  let _out: OutStream
  let _n: I64
  var _next: I64 = 0
  let _fifo: Fifo
  let _parent: Main
  let _t0: (I64,I64)

  new create(out:OutStream, n:I64, fifo:Fifo, parent:Main, t0:(I64,I64)) =>
    _fifo = fifo
    _out = out
    _n = n
    _parent = parent
    _t0 = t0

  be start() =>
    _fifo.consumerRequestsNext(this, _next)

  be consumeThis(prod: Product val) =>
    _next = _next + 1 // should == prod.id + 1
    if _next < _n then
      _fifo.consumerRequestsNext(this, _next)
    else
      // notify parent that we've done what we came to do.
      _parent.consumerDone(_t0)
    end



actor Producer
  let _out: OutStream
  let _n: I64
  let _fifo: Fifo
  var _next: I64 = 0
  let _parent: Main

  new create(out:OutStream, n:I64, fifo:Fifo, parent:Main) =>
    _fifo = fifo
    _out = out
    _n = n
    _parent = parent

  be start() =>
    _fifo.requestToProduce(this, _next)

  be produce(id:I64) =>
    _fifo.append(this, Product(id))
    _next = _next + 1
    if _next < _n then
      _fifo.requestToProduce(this, _next)
    end


