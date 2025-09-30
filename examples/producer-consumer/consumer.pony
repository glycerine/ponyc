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

