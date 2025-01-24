#![allow(clippy::comparison_chain)]
use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    flag: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self { a, b, flag: true };
        if !iter.a.is_valid() {
            iter.flag = false;
            return Ok(iter);
        };
        if !iter.b.is_valid() {
            return Ok(iter);
        };

        if iter.a.key() == iter.b.key() {
            iter.b.next()?;
        } else if iter.a.key() > iter.b.key() {
            iter.flag = false;
        };

        Ok(iter)
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.flag {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.flag {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.flag {
            self.a.next()?;
        } else {
            self.b.next()?;
        }

        if !self.a.is_valid() {
            self.flag = false;
            return Ok(());
        };
        if !self.b.is_valid() {
            self.flag = true;
            return Ok(());
        };

        if self.a.key() < self.b.key() {
            self.flag = true;
        } else if self.a.key() == self.b.key() {
            self.flag = true;
            self.b.next()?;
        } else {
            self.flag = false;
        };

        Ok(())
    }
}
