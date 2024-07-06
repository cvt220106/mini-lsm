use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        // check iters empty
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        let mut binary_heap = BinaryHeap::new();

        // if all iters is invalid,
        if iters.iter().all(|x| !x.is_valid()) {
            let mut iters = iters;
            return Self {
                iters: binary_heap,
                current: Some(HeapWrapper(0, iters.pop().unwrap())),
            };
        }

        // push valid iters to heap
        for (i, v) in iters.into_iter().enumerate() {
            if v.is_valid() {
                binary_heap.push(HeapWrapper(i, v));
            }
        }
        let current = binary_heap.pop().unwrap();
        Self {
            iters: binary_heap,
            current: Some(current),
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        // iter1: b->del, c->4, d->5
        // iter2: a->1, b->2, c->3
        // iter3: e->4
        // a->1, b->del, c->4, d->5, e->4
        // return every key associate latest value
        let current = self.current.as_mut().unwrap();
        // 1. check the same key in other iter, pop this iter
        while let Some(mut inner_iter) = self.iters.peek_mut() {
            debug_assert!(
                inner_iter.1.key() >= current.1.key(),
                "violet iters sequence"
            );

            if inner_iter.1.key() == current.1.key() {
                // Case 1: an error occurred when calling `next`.
                if let e @ Err(_) = inner_iter.1.next() {
                    PeekMut::pop(inner_iter);
                    return e;
                }

                // Case 2: the iter no longer invalid
                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                }
            } else {
                break;
            }
        }

        // current to the next one
        current.1.next()?;
        // 2.check the current validation, if invalid, pop heap to replace
        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current = iter;
            }
            return Ok(());
        }

        // 3.check the current whether is the latest one
        // compare to the heap peek, if heap is smaller, swap it
        if let Some(mut inner_iter) = self.iters.peek_mut() {
            // attention!! actually small one, in our sort mode, is the big one
            // because we use max-heap, so we reverse the sort result
            if *current < *inner_iter {
                std::mem::swap(current, &mut *inner_iter);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters
            .iter()
            .map(|x| x.1.num_active_iterators())
            .sum::<usize>()
            + self
                .current
                .iter()
                .map(|x| x.1.num_active_iterators())
                .sum::<usize>()
    }
}
