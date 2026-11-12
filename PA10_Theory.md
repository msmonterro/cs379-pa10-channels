# PA 10: Channels
Monica Monterrosa Morales | CS 379

## 1. Bounded Buffer Trace

The buffer is shown from oldest to newest. It starts empty and has capacity 2.

| Step | Result | Buffer |
| --- | --- | --- |
| P completes `put(1)` | Adds 1. | `[1]` |
| P completes `put(2)` | Adds 2 and fills the buffer. | `[1, 2]` |
| P attempts `put(3)` | Finds two items in the buffer and waits. The operation is still pending. | `[1, 2]` |
| C completes its first `get()` | Removes and returns 1, freeing a slot and notifying the waiting producer. | `[2]` |
| P completes `put(3)` | Reacquires the lock, sees the free slot, and adds 3. | `[2, 3]` |
| C completes its second `get()` | Removes and returns 2. | `[3]` |

`put(3)` blocks when it checks the buffer under the lock and finds it full. The first `get()` makes progress possible as soon as it removes 1 and notifies the producer. The producer can actually enqueue 3 once it reacquires the lock and confirms the slot is still free.

## 2. Deadlock by Design

| Thread | Blocked on | What would unblock it |
| --- | --- | --- |
| Thread 1 | `ch_a.get()` | Thread 2 putting a value into `ch_a`. |
| Thread 2 | `ch_b.get()` | Thread 1 putting a value into `ch_b`. |

**Wait cycle:** Thread 1 → Thread 2's `ch_a.put()` → Thread 2 → Thread 1's `ch_b.put()` → Thread 1.

Python evaluates the inner `get()` before invoking the outer `put()`. Both channels start empty, so neither thread reaches its `put()`.

To fix this, give Thread 1 an initial value and have it call `ch_b.put(initial_handle)` before calling `ch_a.get()`. Keep Thread 2's original order. The initial put fits in the empty capacity-1 channel. Thread 2 can then receive it and put its response in `ch_a`, which wakes Thread 1. The first send no longer depends on a receive.

## 3. Bounded vs. Unbounded and the GC Connection

Each successful `put()` increments the handle's reference count. The buffer owns that reference, so the value stays alive even after the producer releases its creator reference. When `get()` removes the handle, ownership transfers to the consumer with the same count. The consumer releases that reference with `decref()` after processing it.

If the producer keeps adding values faster than the consumer removes them, an unbounded buffer can retain an ever-growing number of live objects. The objects are still reachable through buffered handles, so reference counting is working as intended when it keeps them alive. A bounded buffer limits how many items can be queued and makes the producer wait when that limit is reached.
