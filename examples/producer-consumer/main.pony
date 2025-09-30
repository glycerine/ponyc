actor Main
  """
  Producer-Consumer concurrency problem.

  Pony has no blocking operations.
  The Pony standard library is structured in this way to use notifier objects,
  callbacks and promises to make programming in this style easier.
  """

  new create(env: Env) =>
    // lots of output:
    // let out = env.out
    // versus: to quiet things day
    let out = NullOutStream
    
    let buffer = Buffer(2, out)

    // at n = 10_000 we see no Assert.crash("good: saw _total") so
    // the program did not actually finish consuming hmm...
    let n:U32 = 10_000  // bad: no assert at consumer.pony:90 !?!
    let producer = Producer(n, buffer, out)
    let consumer = Consumer(n, buffer, out)

    consumer.start_consuming()
    producer.start_producing()

    env.out.print("**Main** Finished.")


actor NullOutStream is OutStream
  be print(data: ByteSeq) => None
  be write(data: ByteSeq) => None
  be printv(data: ByteSeqIter) => None
  be writev(data: ByteSeqIter) => None
  fun ref apply(data: ByteSeq) => None
  be flush() => None
