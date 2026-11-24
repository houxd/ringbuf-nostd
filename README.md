# ringbuf-nostd

A fixed-capacity, lock-free **single-producer / single-consumer (SPSC)** ring buffer for `no_std` targets.

- Capacity is a compile-time const generic `N`; no heap allocation is needed.
- Depends only on `core` and `portable-atomic`, so it builds for bare-metal targets such as `thumbv6m-none-eabi` (Cortex-M0/M0+), `thumbv7em-none-eabihf` (Cortex-M4), and RISC-V.
- The producer and consumer can run concurrently (for example, a main loop and an interrupt handler) without disabling interrupts. They synchronize with atomic `Release` / `Acquire` ordering.
- Also works on `std` hosts (for example, `cargo test` or the host-side emulator).

## Quick start

```rust
use ringbuf_nostd::RingBuf;

let mut rb: RingBuf<u8, 8> = RingBuf::new();
let (mut producer, mut consumer) = rb.split();

assert!(producer.push(1));
assert!(producer.push(2));
assert_eq!(consumer.pop(), Some(1));
assert_eq!(consumer.len(), 1);

let mut out = [0u8; 4];
let n = consumer.copy_to(&mut out); // copies without removing
assert_eq!(&out[..n], &[2]);
```

## Design

`RingBuf::split(&mut self)` returns two handles, which enforces the SPSC contract at the type level:

| Handle | Owns | Methods |
| --- | --- | --- |
| `Producer<'_, T, N>` | write index `head` | `push`, `capacity`, `len`, `is_empty`, `is_full` |
| `Consumer<'_, T, N>` | read index `tail` | `pop`, `front`, `tail`, `copy_to`, `iter`, `clear`, `is_continuous`, `capacity`, `len`, `is_empty`, `is_full` |

- The `&mut` borrow lasts as long as the handles do. The buffer cannot be split twice at the same time, and it can be split again after both handles are dropped.
- `Producer` is the only writer of `head`, and `Consumer` is the only writer of `tail`. Each side reads the other's index with `Acquire` and publishes its own index with `Release`, so element data is visible before the index that announces it.
- `len`, `is_empty` and `is_full` are snapshots. They may be stale while the other side is active. While the handles exist, use the handles' methods; the `RingBuf` itself is mutably borrowed.

## Platform support

| Target | Examples | Extra setup |
| --- | --- | --- |
| Cortex-M0 / M0+ (`thumbv6m-none-eabi`) | STM32F0, STM32L0 | None |
| Cortex-M3 / M4 / M7 (`thumbv7m`, `thumbv7em`) | STM32F1, STM32F4, STM32F7 | None |
| RISC-V, other targets | — | None |

The crate uses only atomic load and store, which `portable-atomic` provides on every target above. It does not use compare-and-swap, so no `critical-section` implementation is required, and it has no feature flags.

## Type constraints

| Constraint | Notes |
| --- | --- |
| `T: Copy` | Elements are copied in and out. |
| All-zero bit pattern of `T` is valid | `new()` zero-initializes the storage. `u8`, `i32`, `bool`, `f32` are fine. `&T`, `NonNull<T>`, and enums whose zero value is not a variant are **not**; using them is undefined behavior. |
| `T: Send` for `Sync` | `RingBuf` is `Sync` only when `T: Send`. |
| `N > 1` | `new()` panics at compile time (const evaluation) otherwise. |

Capacity is `N - 1`: one slot is sacrificed to distinguish "empty" from "full". For example, `RingBuf<u8, 8>` holds at most 7 elements.

## API

### `RingBuf<T, N>`

| Method | Description |
| --- | --- |
| `const fn new() -> Self` | Creates an empty buffer. |
| `fn split(&mut self) -> (Producer, Consumer)` | Splits the buffer into the two handles. |
| `const fn capacity(&self) -> usize` | Maximum number of elements (`N - 1`). |
| `fn len(&self) -> usize` | Number of stored elements (snapshot). |
| `fn is_empty(&self) -> bool` | Snapshot. |
| `fn is_full(&self) -> bool` | Snapshot. |

### `Producer<'_, T, N>`

| Method | Description |
| --- | --- |
| `fn push(&mut self, value: T) -> bool` | Appends an element. Returns `false` if full; existing data is never overwritten. |
| `fn capacity`, `len`, `is_empty`, `is_full` | Same as on `RingBuf`. |

### `Consumer<'_, T, N>`

| Method | Description |
| --- | --- |
| `fn pop(&mut self) -> Option<T>` | Removes and returns the oldest element. |
| `fn front(&self) -> Option<&T>` | Reference to the oldest element (the next one `pop` returns). |
| `fn tail(&self) -> Option<&T>` | Reference to the most recently pushed element. |
| `fn copy_to(&self, dest: &mut [T]) -> usize` | Copies up to `dest.len()` elements from the front without removing them. Returns the number copied. |
| `fn iter(&self) -> RingBufIter` | Iterates oldest to newest without consuming. Borrows the consumer, so `pop` and `clear` cannot run during iteration. |
| `fn clear(&mut self)` | Discards all elements. |
| `fn is_continuous(&self) -> bool` | `true` if the elements do not wrap past the end of the array. |
| `fn capacity`, `len`, `is_empty`, `is_full` | Same as on `RingBuf`. |

## Example: UART receive from an interrupt

Because `split` needs `&mut`, put the buffer in a `static mut` and split it once during initialization, before the interrupt is enabled:

```rust
use ringbuf_nostd::{Consumer, Producer, RingBuf};

static mut RX: RingBuf<u8, 256> = RingBuf::new();

// Called once from main, before the UART interrupt is enabled.
// SAFETY: runs exactly once before any other access to RX.
fn init() -> (Producer<'static, u8, 256>, Consumer<'static, u8, 256>) {
    let rb: &'static mut RingBuf<u8, 256> = unsafe { &mut *core::ptr::addr_of_mut!(RX) };
    rb.split()
}
```

Hand the `Producer` to the interrupt handler and keep the `Consumer` for the main loop. How you share the producer with the ISR depends on your platform. A common approach is a `critical_section::Mutex<RefCell<Option<Producer>>>` that is written once during init. The interrupt handler only calls `push`:

```rust
fn on_rx_byte(producer: &mut Producer<'static, u8, 256>, byte: u8) {
    let _ = producer.push(byte); // drop the byte if the buffer is full
}
```

In the main loop, use the consumer with `pop`, or with `copy_to` followed by `pop`s. No critical section is needed around the buffer itself, because each side owns its own index.

> Only **one** producer and **one** consumer may exist. If several contexts push (or several pop), wrap the operation in a critical section or redesign the data flow.

## Tests

```bash
cargo test -p ringbuf-nostd
```

The tests are in `tests/ringbuf.rs`. They cover:

- empty and full states, capacity (`N - 1`), and rejection of writes when full;
- FIFO order, `front` / `tail`, and correct behavior after index wrap-around;
- `clear`, `is_continuous`, `iter`;
- `copy_to` for contiguous data, data that wraps past the array end, and destination slices of different sizes;
- `split` again after the handles are dropped, and a buffer leaked to `'static` and split;
- minimum size `N = 2`, struct elements, and a panic for `N = 1`;
- a deterministic randomized model check against `VecDeque`, for `N` from 2 to 1024;
- a two-thread SPSC run that checks every value arrives exactly once and in order.

Bare-metal build checks:

```bash
cargo build -p ringbuf-nostd --target thumbv6m-none-eabi
cargo build -p ringbuf-nostd --target thumbv7em-none-eabihf
```
