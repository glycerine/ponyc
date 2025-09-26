use @pony_exitcode[None](code: I32)

class Foo
  fun fn(x: I32) => None

actor Main
  fun foo(): Foo ? => error
  new create(env: Env) => 

    var x: I32 = 0
    try
      // This call should fail on foo() before fn() is ever reached.
      // The bug is that `x = 42` is evaluated as a side-effecting
      // assignment before the receiver `foo()?` is evaluated.
      foo()?.fn(x = 42)
    end
    
    // With the bug, x will be 42.
    // With the fix, the assignment will be part of the call that never
    // happens, so x should remain 0.
    @pony_exitcode(x)    

