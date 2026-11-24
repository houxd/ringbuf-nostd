use ringbuf_nostd::RingBuf;
use std::collections::VecDeque;
use std::hint::spin_loop;
use std::thread;

#[derive(Clone, Copy, Default, Debug, PartialEq)]
struct Pair {
    a: u32,
    b: i16,
}

#[test]
fn new_buffer_is_empty() {
    let mut rb: RingBuf<u8, 8> = RingBuf::new();
    assert_eq!(rb.capacity(), 7);
    assert!(rb.is_empty());
    assert!(!rb.is_full());
    assert_eq!(rb.len(), 0);

    let (mut p, mut c) = rb.split();
    assert_eq!(p.capacity(), 7);
    assert!(p.is_empty());
    assert_eq!(c.front(), None);
    assert_eq!(c.tail(), None);
    assert!(c.is_continuous());
    assert_eq!(c.iter().count(), 0);
    assert_eq!(c.pop(), None);
    assert!(p.push(1));
}

#[test]
fn split_again_after_handles_dropped() {
    let mut rb: RingBuf<u8, 4> = RingBuf::new();
    {
        let (mut p, _c) = rb.split();
        p.push(1);
        p.push(2);
    }
    // Handles are gone; the buffer can be split again and keeps its contents.
    let (_p, mut c) = rb.split();
    assert_eq!(c.len(), 2);
    assert_eq!(c.pop(), Some(1));
}

#[test]
fn capacity_is_n_minus_one() {
    let mut rb: RingBuf<u32, 5> = RingBuf::new();
    let (mut p, c) = rb.split();
    for i in 0..4 {
        assert!(p.push(i), "push {i} should succeed");
    }
    assert!(p.is_full());
    assert_eq!(c.len(), 4);
    assert!(!p.push(99), "push into full buffer must fail");
    assert_eq!(c.len(), 4);
}

#[test]
fn push_when_full_does_not_overwrite() {
    let mut rb: RingBuf<u8, 4> = RingBuf::new();
    let (mut p, c) = rb.split();
    assert!(p.push(1));
    assert!(p.push(2));
    assert!(p.push(3));
    assert!(!p.push(4));
    assert_eq!(c.front(), Some(&1));
    assert_eq!(c.tail(), Some(&3));
    let items: Vec<u8> = c.iter().copied().collect();
    assert_eq!(items, vec![1, 2, 3]);
}

#[test]
fn fifo_order_is_preserved() {
    let mut rb: RingBuf<u8, 8> = RingBuf::new();
    let (mut p, mut c) = rb.split();
    for i in 1..=5 {
        assert!(p.push(i));
    }
    for i in 1..=5 {
        assert_eq!(c.pop(), Some(i));
    }
    assert_eq!(c.pop(), None);
    assert!(c.is_empty());
}

#[test]
fn front_and_tail_follow_ends() {
    let mut rb: RingBuf<u8, 8> = RingBuf::new();
    let (mut p, mut c) = rb.split();
    p.push(10);
    assert_eq!(c.front(), Some(&10));
    assert_eq!(c.tail(), Some(&10));
    p.push(20);
    p.push(30);
    assert_eq!(c.front(), Some(&10));
    assert_eq!(c.tail(), Some(&30));
    c.pop();
    assert_eq!(c.front(), Some(&20));
    assert_eq!(c.tail(), Some(&30));
}

#[test]
fn tail_when_head_wrapped_to_zero() {
    // N = 4: push 3 -> head = 3; pop 3 -> tail = 3; push 2 -> head wraps to 1.
    let mut rb: RingBuf<u8, 4> = RingBuf::new();
    let (mut p, mut c) = rb.split();
    for i in 0..3 {
        p.push(i);
    }
    for _ in 0..3 {
        c.pop();
    }
    assert!(p.push(7)); // slot 3, head -> 0
    assert!(p.push(8)); // slot 0, head -> 1
    assert_eq!(c.tail(), Some(&8));
    assert_eq!(c.front(), Some(&7));
    assert_eq!(c.len(), 2);
}

#[test]
fn len_is_correct_across_wrap() {
    let mut rb: RingBuf<u8, 4> = RingBuf::new();
    let (mut p, mut c) = rb.split();
    for round in 0..10u8 {
        assert_eq!(c.len(), 0);
        p.push(round);
        p.push(round);
        assert_eq!(p.len(), 2);
        c.pop();
        assert_eq!(c.len(), 1);
        c.pop();
    }
}

#[test]
fn wrap_around_many_times() {
    let mut rb: RingBuf<u16, 5> = RingBuf::new();
    let (mut p, mut c) = rb.split();
    let mut next_in = 0u16;
    let mut next_out = 0u16;
    for _ in 0..100 {
        while p.push(next_in) {
            next_in += 1;
        }
        for _ in 0..3 {
            assert_eq!(c.pop(), Some(next_out));
            next_out += 1;
        }
    }
    while let Some(v) = c.pop() {
        assert_eq!(v, next_out);
        next_out += 1;
    }
    assert_eq!(next_out, next_in);
}

#[test]
fn clear_empties_buffer_and_is_reusable() {
    let mut rb: RingBuf<u8, 4> = RingBuf::new();
    let (mut p, mut c) = rb.split();
    p.push(1);
    p.push(2);
    c.clear();
    assert!(c.is_empty());
    assert_eq!(c.len(), 0);
    assert_eq!(c.pop(), None);
    assert!(p.push(5));
    assert_eq!(c.pop(), Some(5));
}

#[test]
fn clear_full_buffer() {
    let mut rb: RingBuf<u8, 4> = RingBuf::new();
    let (mut p, mut c) = rb.split();
    p.push(1);
    p.push(2);
    p.push(3);
    c.clear();
    assert!(c.is_empty());
    assert!(!c.is_full());
    assert_eq!(c.len(), 0);
    assert_eq!(c.capacity(), 3);
    for i in 0..3 {
        assert!(p.push(i));
    }
    assert!(p.is_full());
}

#[test]
fn iterator_yields_in_order_without_consuming() {
    let mut rb: RingBuf<u8, 4> = RingBuf::new();
    let (mut p, mut c) = rb.split();
    // Move head/tail to 2 so the data wraps around the end of the array.
    p.push(0);
    p.push(0);
    c.pop();
    c.pop();
    p.push(1);
    p.push(2);
    p.push(3);

    let first: Vec<u8> = c.iter().copied().collect();
    assert_eq!(first, vec![1, 2, 3]);
    let second: Vec<u8> = c.iter().copied().collect();
    assert_eq!(second, vec![1, 2, 3]);
    assert_eq!(c.len(), 3);
    assert_eq!(c.iter().len(), 3);
}

#[test]
fn iterator_on_full_buffer_is_ordered() {
    let mut rb: RingBuf<u8, 4> = RingBuf::new();
    let (mut p, mut c) = rb.split();
    p.push(0);
    p.push(0);
    c.pop();
    c.pop();
    assert!(p.push(1));
    assert!(p.push(2));
    assert!(p.push(3));
    assert!(p.is_full());
    let items: Vec<u8> = c.iter().copied().collect();
    assert_eq!(items, vec![1, 2, 3]);
}

#[test]
fn copy_to_contiguous() {
    let mut rb: RingBuf<u8, 8> = RingBuf::new();
    let (mut p, c) = rb.split();
    for i in 1..=4 {
        p.push(i);
    }
    assert!(c.is_continuous());
    let mut dest = [0u8; 4];
    assert_eq!(c.copy_to(&mut dest), 4);
    assert_eq!(dest, [1, 2, 3, 4]);
    assert_eq!(c.len(), 4, "copy_to must not consume");
}

#[test]
fn copy_to_wrapped() {
    let mut rb: RingBuf<u8, 5> = RingBuf::new();
    let (mut p, mut c) = rb.split();
    for i in 0..4 {
        p.push(i);
    }
    c.pop();
    c.pop();
    p.push(10);
    p.push(11); // physical: [11, _, 2, 3, 10]; logical: 2, 3, 10, 11
    assert!(!c.is_continuous());
    let mut dest = [0u8; 4];
    assert_eq!(c.copy_to(&mut dest), 4);
    assert_eq!(dest, [2, 3, 10, 11]);
}

#[test]
fn copy_to_wrapped_boundary_exact() {
    let mut rb: RingBuf<u8, 5> = RingBuf::new();
    let (mut p, mut c) = rb.split();
    for i in 0..4 {
        p.push(i);
    }
    c.pop();
    c.pop();
    c.pop();
    p.push(9); // head wraps 4 -> 0
    p.push(8); // head = 1
    // Elements 3, 9, 8 sit at physical indices 3, 4, 0; the copy crosses the end.
    let mut dest = [0u8; 2];
    assert_eq!(c.copy_to(&mut dest), 2);
    assert_eq!(dest, [3, 9]);
    let mut dest = [0u8; 3];
    assert_eq!(c.copy_to(&mut dest), 3);
    assert_eq!(dest, [3, 9, 8]);
}

#[test]
fn copy_to_dest_smaller_than_len() {
    let mut rb: RingBuf<u8, 8> = RingBuf::new();
    let (mut p, c) = rb.split();
    for i in 1..=6 {
        p.push(i);
    }
    let mut dest = [0u8; 2];
    assert_eq!(c.copy_to(&mut dest), 2);
    assert_eq!(dest, [1, 2]);
}

#[test]
fn copy_to_dest_larger_than_len() {
    let mut rb: RingBuf<u8, 8> = RingBuf::new();
    let (mut p, c) = rb.split();
    p.push(7);
    p.push(8);
    let mut dest = [0xAAu8; 6];
    assert_eq!(c.copy_to(&mut dest), 2);
    assert_eq!(dest, [7, 8, 0xAA, 0xAA, 0xAA, 0xAA]);
}

#[test]
fn copy_to_empty_or_zero_dest() {
    let mut rb: RingBuf<u8, 4> = RingBuf::new();
    let (mut p, c) = rb.split();
    let mut dest = [1u8; 3];
    assert_eq!(c.copy_to(&mut dest), 0);
    assert_eq!(dest, [1, 1, 1]);

    p.push(5);
    let mut empty: [u8; 0] = [];
    assert_eq!(c.copy_to(&mut empty), 0);
}

#[test]
fn is_continuous_reflects_layout() {
    let mut rb: RingBuf<u8, 4> = RingBuf::new();
    let (mut p, mut c) = rb.split();
    assert!(c.is_continuous());
    p.push(1);
    p.push(2);
    p.push(3); // head = 3, tail = 0
    assert!(c.is_continuous());
    c.pop(); // tail = 1
    p.push(4); // head wraps 3 -> 0; tail = 1 > head = 0
    assert!(!c.is_continuous());
}

#[test]
fn smallest_buffer_n2() {
    let mut rb: RingBuf<u8, 2> = RingBuf::new();
    let (mut p, mut c) = rb.split();
    assert_eq!(c.capacity(), 1);
    assert!(p.push(42));
    assert!(p.is_full());
    assert!(!p.push(43));
    assert_eq!(c.front(), Some(&42));
    assert_eq!(c.pop(), Some(42));
    assert!(c.is_empty());
    for i in 0..10u8 {
        assert!(p.push(i));
        assert_eq!(c.pop(), Some(i));
    }
}

#[test]
#[should_panic]
fn n_must_be_greater_than_one() {
    let _rb: RingBuf<u8, 1> = RingBuf::new();
}

#[test]
fn copy_type_struct_elements() {
    let mut rb: RingBuf<Pair, 4> = RingBuf::new();
    let (mut p, mut c) = rb.split();
    let p1 = Pair { a: 1, b: -1 };
    let p2 = Pair { a: 2, b: -2 };
    p.push(p1);
    p.push(p2);
    assert_eq!(c.front(), Some(&p1));
    assert_eq!(c.tail(), Some(&p2));
    assert_eq!(c.pop(), Some(p1));
    assert_eq!(c.pop(), Some(p2));
}

#[test]
fn leaked_static_style_buffer_can_be_split() {
    // Mirrors the `static mut` pattern from the crate docs.
    let rb: &'static mut RingBuf<u8, 8> = Box::leak(Box::new(RingBuf::new()));
    let (mut p, mut c) = rb.split();
    assert!(p.push(3));
    assert_eq!(c.pop(), Some(3));
}

/// Deterministic pseudo-random sequence (xorshift) so the test is reproducible.
struct XorShift(u64);
impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

fn model_check<const N: usize>(seed: u64, steps: usize) {
    let mut rb: RingBuf<u32, N> = RingBuf::new();
    let (mut p, mut c) = rb.split();
    let mut model: VecDeque<u32> = VecDeque::new();
    let cap = N - 1;
    let mut rng = XorShift(seed);
    let mut counter = 0u32;

    for step in 0..steps {
        match rng.next() % 10 {
            0..=4 => {
                counter = counter.wrapping_add(1);
                let ok = p.push(counter);
                if model.len() < cap {
                    assert!(ok, "step {step}: push should succeed");
                    model.push_back(counter);
                } else {
                    assert!(!ok, "step {step}: push should fail when full");
                }
            }
            5..=7 => {
                assert_eq!(c.pop(), model.pop_front(), "step {step}: pop mismatch");
            }
            8 => {
                let len = (rng.next() as usize) % (cap + 2);
                let mut dest = vec![0u32; len];
                let n = c.copy_to(&mut dest);
                let expect: Vec<u32> = model.iter().take(len).copied().collect();
                assert_eq!(n, expect.len(), "step {step}: copy_to count");
                assert_eq!(&dest[..n], &expect[..], "step {step}: copy_to data");
            }
            _ => {
                if rng.next().is_multiple_of(4) {
                    c.clear();
                    model.clear();
                } else {
                    let items: Vec<u32> = c.iter().copied().collect();
                    let expect: Vec<u32> = model.iter().copied().collect();
                    assert_eq!(items, expect, "step {step}: iter mismatch");
                }
            }
        }

        assert_eq!(c.len(), model.len(), "step {step}: len");
        assert_eq!(p.len(), model.len(), "step {step}: producer len");
        assert_eq!(c.is_empty(), model.is_empty(), "step {step}: is_empty");
        assert_eq!(p.is_full(), model.len() == cap, "step {step}: is_full");
        assert_eq!(c.front(), model.front(), "step {step}: front");
        assert_eq!(c.tail(), model.back(), "step {step}: tail");
    }
}

#[test]
fn model_check_small_buffers() {
    model_check::<2>(0x1234_5678, 5_000);
    model_check::<3>(0xdead_beef, 5_000);
}

#[test]
fn model_check_medium_buffers() {
    model_check::<8>(0x0bad_cafe, 20_000);
    model_check::<16>(0x9e37_79b9, 20_000);
}

#[test]
fn model_check_large_buffer() {
    model_check::<1024>(0x7f4a_7c15, 50_000);
}

/// Real two-thread SPSC run: the producer and consumer race on the atomics, and every
/// value must arrive exactly once and in order.
#[test]
fn spsc_two_threads_preserve_order() {
    const COUNT: u32 = 200_000;
    let mut rb: RingBuf<u32, 64> = RingBuf::new();
    let (mut p, mut c) = rb.split();

    thread::scope(|s| {
        s.spawn(move || {
            for i in 0..COUNT {
                while !p.push(i) {
                    spin_loop();
                }
            }
        });
        s.spawn(move || {
            let mut expect = 0u32;
            while expect < COUNT {
                match c.pop() {
                    Some(v) => {
                        assert_eq!(v, expect);
                        expect += 1;
                    }
                    None => spin_loop(),
                }
            }
            assert_eq!(c.pop(), None);
        });
    });

    // Both handles were moved into the threads and dropped, so the buffer is usable again.
    let (_p, c) = rb.split();
    assert!(c.is_empty());
}
