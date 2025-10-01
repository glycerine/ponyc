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
// Here is one solution.
