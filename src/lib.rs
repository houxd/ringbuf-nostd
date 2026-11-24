//! A fixed-capacity, lock-free single-producer / single-consumer (SPSC) ring buffer.
//!
//! [`RingBuf::split`] takes `&mut self` and returns two handles:
//!
//! - [`Producer`] owns the write side (`head`) and is the only one that calls `push`.
//! - [`Consumer`] owns the read side (`tail`) and is the only one that calls `pop`,
//!   `clear`, `front`, `tail`, `copy_to` and `iter`.
//!
//! The `&mut` borrow stays alive as long as the handles do, so the buffer cannot be
//! split twice. Because each index is written by exactly one side, and the data is
//! published with `Release` / `Acquire` ordering, the two sides can run concurrently
//! (for example a main loop and an interrupt handler) without disabling interrupts.
//!
//! Atomics come from [`portable-atomic`](https://crates.io/crates/portable-atomic).
//! Only atomic load and store are used, so no feature flags are needed, and the crate
//! builds on Cortex-M0 / `thumbv6m` as well as Cortex-M3/M4/M7 and RISC-V.
//!
//! ```
//! use ringbuf_nostd::RingBuf;
//!
//! let mut rb: RingBuf<u8, 8> = RingBuf::new();
//! let (mut producer, mut consumer) = rb.split();
//! assert!(producer.push(1));
//! assert_eq!(consumer.pop(), Some(1));
//! ```
//!
//! For a buffer shared with an interrupt handler, place it in a `static mut` and split
//! it once during initialization, before the interrupt is enabled:
//!
//! ```
//! use ringbuf_nostd::RingBuf;
//!
//! static mut RB: RingBuf<u8, 8> = RingBuf::new();
//!
//! // SAFETY: called once, before any other code accesses `RB`.
//! let rb: &'static mut RingBuf<u8, 8> = unsafe { &mut *core::ptr::addr_of_mut!(RB) };
//! let (mut producer, mut consumer) = rb.split();
//! assert!(producer.push(1));
//! assert_eq!(consumer.pop(), Some(1));
//! ```

#![cfg_attr(not(test), no_std)]

use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use core::ptr;
use portable_atomic::{AtomicUsize, Ordering};

/// Fixed-capacity SPSC ring buffer with `N - 1` usable slots.
///
/// Constraints:
/// - `T` must be `Copy`;
/// - the all-zero bit pattern of `T` must be a valid value, because [`RingBuf::new`]
///   zero-initializes the storage. `u8`, `i32`, `bool`, `f32` are fine; `&T`,
///   `NonNull<T>` and enums whose zero value is not a variant are not;
/// - `N` must be greater than 1.
pub struct RingBuf<T, const N: usize> {
    buf: UnsafeCell<[T; N]>,
    /// Write index. Only written by [`Producer`].
    head: AtomicUsize,
    /// Read index. Only written by [`Consumer`].
    tail: AtomicUsize,
}

// SAFETY: element access is coordinated by the atomic indices. The producer only
// writes slots that the consumer does not currently own, and the consumer only reads
// slots that the producer has published. Elements are moved between threads, so
// `T: Send` is required.
unsafe impl<T: Send, const N: usize> Sync for RingBuf<T, N> {}

/// Number of elements between `tail` (read index) and `head` (write index).
#[inline]
const fn distance(tail: usize, head: usize, n: usize) -> usize {
    if head >= tail {
        head - tail
    } else {
        n - tail + head
    }
}

impl<T: Copy, const N: usize> RingBuf<T, N> {
    /// Creates an empty buffer. `N` must be greater than 1.
    ///
    /// Storage is zero-initialized (see the type-level constraints).
    pub const fn new() -> Self {
        assert!(N > 1, "RingBuf size must be greater than 1");
        Self {
            buf: UnsafeCell::new(unsafe { MaybeUninit::<[T; N]>::zeroed().assume_init() }),
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    /// Maximum number of elements the buffer can hold (`N - 1`).
    pub const fn capacity(&self) -> usize {
        N - 1
    }

    /// Returns the number of elements currently stored.
    ///
    /// The value is a snapshot; while a producer or consumer is active it may be
    /// stale by the time it is read. Once [`RingBuf::split`] has been called, use the
    /// handles' `len` instead, because the buffer itself is mutably borrowed.
    pub fn len(&self) -> usize {
        // Read `tail` before `head`: the consumer never advances `tail` past the
        // `head` it observed, so this ordering keeps the distance non-negative.
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);
        distance(tail, head, N)
    }

    /// Returns `true` if the buffer holds no elements (snapshot).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if the buffer is full (snapshot).
    pub fn is_full(&self) -> bool {
        self.len() == N - 1
    }

    /// Splits the buffer into a [`Producer`] and a [`Consumer`].
    ///
    /// The returned handles keep the `&mut` borrow alive, so the buffer can only be
    /// split again after both handles are dropped.
    pub fn split(&mut self) -> (Producer<'_, T, N>, Consumer<'_, T, N>) {
        let rb: &RingBuf<T, N> = self;
        (Producer { rb }, Consumer { rb })
    }

    /// Raw pointer to slot `idx`. The caller must ensure `idx < N` and must respect
    /// the producer/consumer ownership of that slot.
    #[inline]
    fn slot(&self, idx: usize) -> *mut T {
        // This only computes a pointer; dereferencing is done by callers under the
        // ownership rules above.
        unsafe { (self.buf.get() as *mut T).add(idx) }
    }
}

/// Write side of a [`RingBuf`]. Only one producer exists per buffer.
pub struct Producer<'a, T, const N: usize> {
    rb: &'a RingBuf<T, N>,
}

impl<'a, T: Copy, const N: usize> Producer<'a, T, N> {
    /// Appends `value`. Returns `false` if the buffer is full; existing data is never
    /// overwritten.
    pub fn push(&mut self, value: T) -> bool {
        let rb = self.rb;
        // Only the producer writes `head`, so a relaxed load is sufficient.
        let head = rb.head.load(Ordering::Relaxed);
        let next = (head + 1) % N;
        // Acquire pairs with the consumer's Release store of `tail`, so the consumer's
        // read of this slot happens-before our overwrite.
        if next == rb.tail.load(Ordering::Acquire) {
            return false;
        }
        // SAFETY: `head < N`, and the slot is not owned by the consumer (it lies outside
        // `[tail, head)`).
        unsafe { ptr::write(rb.slot(head), value) };
        // Publish the element: the consumer's Acquire load of `head` sees the write.
        rb.head.store(next, Ordering::Release);
        true
    }

    /// Returns the maximum number of elements the buffer can hold (`N - 1`).
    pub fn capacity(&self) -> usize {
        N - 1
    }

    /// Returns `true` if the buffer is full (snapshot).
    pub fn is_full(&self) -> bool {
        self.rb.is_full()
    }

    /// Returns the number of elements currently stored (snapshot).
    pub fn len(&self) -> usize {
        self.rb.len()
    }

    /// Returns `true` if the buffer is empty (snapshot).
    pub fn is_empty(&self) -> bool {
        self.rb.is_empty()
    }
}

/// Read side of a [`RingBuf`]. Only one consumer exists per buffer.
pub struct Consumer<'a, T, const N: usize> {
    rb: &'a RingBuf<T, N>,
}

impl<'a, T: Copy, const N: usize> Consumer<'a, T, N> {
    /// Removes and returns the oldest element, or `None` if the buffer is empty.
    pub fn pop(&mut self) -> Option<T> {
        let rb = self.rb;
        // Only the consumer writes `tail`, so a relaxed load is sufficient.
        let tail = rb.tail.load(Ordering::Relaxed);
        // Acquire pairs with the producer's Release store of `head`, so the element
        // written by `push` is visible here.
        if tail == rb.head.load(Ordering::Acquire) {
            return None;
        }
        // SAFETY: `tail < N`, and `[tail, head)` holds initialized elements owned by
        // the consumer.
        let value = unsafe { ptr::read(rb.slot(tail)) };
        // Release the slot back to the producer.
        rb.tail.store((tail + 1) % N, Ordering::Release);
        Some(value)
    }

    /// Returns a reference to the oldest element (next to be popped).
    pub fn front(&self) -> Option<&T> {
        let rb = self.rb;
        let tail = rb.tail.load(Ordering::Relaxed);
        if tail == rb.head.load(Ordering::Acquire) {
            None
        } else {
            // SAFETY: the slot lies in `[tail, head)` and cannot be overwritten until
            // the consumer pops it, which requires `&mut self`.
            Some(unsafe { &*rb.slot(tail) })
        }
    }

    /// Returns a reference to the most recently pushed element.
    pub fn tail(&self) -> Option<&T> {
        let rb = self.rb;
        let tail = rb.tail.load(Ordering::Relaxed);
        let head = rb.head.load(Ordering::Acquire);
        if tail == head {
            None
        } else {
            let idx = if head == 0 { N - 1 } else { head - 1 };
            // SAFETY: slot `head - 1` is in `[tail, head)`, so it is owned by the consumer.
            Some(unsafe { &*rb.slot(idx) })
        }
    }

    /// Discards all elements. The buffer may be refilled afterwards.
    pub fn clear(&mut self) {
        let head = self.rb.head.load(Ordering::Acquire);
        self.rb.tail.store(head, Ordering::Release);
    }

    /// Copies up to `dest.len()` elements from the front into `dest`, without removing
    /// them. Returns the number of elements copied.
    pub fn copy_to(&self, dest: &mut [T]) -> usize {
        let rb = self.rb;
        let tail = rb.tail.load(Ordering::Relaxed);
        let head = rb.head.load(Ordering::Acquire);
        let n = dest.len().min(distance(tail, head, N));
        if n == 0 {
            return 0;
        }
        let base = rb.slot(0);
        // Copy in at most two segments, split at the end of the physical array.
        let first = n.min(N - tail);
        // SAFETY: all source ranges lie within `[tail, head)` and are therefore
        // initialized and owned by the consumer; `dest` has at least `n` elements and
        // does not overlap the buffer (it is a `&mut` borrow).
        unsafe {
            ptr::copy_nonoverlapping(base.add(tail), dest.as_mut_ptr(), first);
            ptr::copy_nonoverlapping(base, dest.as_mut_ptr().add(first), n - first);
        }
        n
    }

    /// Iterates from the oldest to the newest element without removing them.
    ///
    /// The iterator borrows the consumer, so `pop` and `clear` cannot run during
    /// iteration.
    pub fn iter(&self) -> RingBufIter<'_, T, N> {
        let rb = self.rb;
        let tail = rb.tail.load(Ordering::Relaxed);
        let head = rb.head.load(Ordering::Acquire);
        RingBufIter {
            rb,
            current: tail,
            remaining: distance(tail, head, N),
        }
    }

    /// Returns the maximum number of elements the buffer can hold (`N - 1`).
    pub fn capacity(&self) -> usize {
        N - 1
    }

    /// Returns the number of elements currently stored (snapshot).
    pub fn len(&self) -> usize {
        self.rb.len()
    }

    /// Returns `true` if the buffer is empty (snapshot).
    pub fn is_empty(&self) -> bool {
        self.rb.is_empty()
    }

    /// Returns `true` if the buffer is full (snapshot).
    pub fn is_full(&self) -> bool {
        self.rb.is_full()
    }

    /// Returns `true` if the stored elements are contiguous in memory, i.e. they do not
    /// wrap past the end of the array.
    pub fn is_continuous(&self) -> bool {
        let tail = self.rb.tail.load(Ordering::Relaxed);
        let head = self.rb.head.load(Ordering::Acquire);
        tail <= head
    }
}

/// Iterator over the elements of a [`Consumer`], oldest first.
pub struct RingBufIter<'a, T, const N: usize> {
    rb: &'a RingBuf<T, N>,
    current: usize,
    remaining: usize,
}

impl<'a, T: Copy, const N: usize> Iterator for RingBufIter<'a, T, N> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        // SAFETY: `current` lies in the snapshot range `[tail, head)` taken when the
        // iterator was created. The consumer is borrowed for the iterator's lifetime,
        // so these slots are not popped, and the producer cannot overwrite them.
        let item = unsafe { &*self.rb.slot(self.current) };
        self.current = (self.current + 1) % N;
        self.remaining -= 1;
        Some(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<'a, T: Copy, const N: usize> ExactSizeIterator for RingBufIter<'a, T, N> {}
