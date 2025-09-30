
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
    



actor Consumer
  let _total: U32
  var _quantity_to_consume: U32
  let _buffer: Buffer
  let _out: OutStream
  var _expect: U32 = 0

  new create(quantity_to_consume: U32, buffer: Buffer, out: OutStream) =>
    _total = quantity_to_consume
    _quantity_to_consume = quantity_to_consume
    _buffer = buffer
    _out = out

  be start_consuming(count: U32 = 0) =>
    _buffer.permission_to_consume(this)
    if count < _quantity_to_consume then
      start_consuming(count + 1)
    end

  be consuming(product: Product) =>
    if product.id != _expect then
      _out.print("bad: expected " + _expect.string() + ", but got " + product.id.string())
      Assert.crash("bad: expected " + _expect.string() + ", but got " + product.id.string())
    end
    if _expect == _total then
      //None
      Assert.crash("good: saw _total = " + _total.string())
    end
    _out.print("_expect = " + _expect.string() + " total = " + _total.string()+" saw "+product.id.string())
    _expect = _expect + 1
    _out.print("**Consumer** Consuming product " + product.id.string())
    _quantity_to_consume = _quantity_to_consume -1

