/// Boolean iterators: AND, OR, NOT.
/// Compose PostingIterators into query evaluation trees.

use crate::postings::{PostingIterator, NO_MORE_DOCS};

/// AND iterator: yields documents present in ALL children.
/// Drives iteration from the rarest term (smallest df).
pub struct AndIterator<'a> {
    children: Vec<Box<dyn PostingIterator + 'a>>,
    cur_doc_id: u32,
}

impl<'a> AndIterator<'a> {
    pub fn new(mut children: Vec<Box<dyn PostingIterator + 'a>>) -> Self {
        // Sort by df ascending so rarest term drives iteration
        children.sort_by_key(|c| c.doc_freq());
        Self {
            children,
            cur_doc_id: NO_MORE_DOCS,
        }
    }
}

impl<'a> PostingIterator for AndIterator<'a> {
    fn next_doc(&mut self) -> Option<u32> {
        if self.children.is_empty() {
            return None;
        }

        // Start by advancing the lead (rarest) iterator
        let lead_doc = match self.children[0].next_doc() {
            Some(d) => d,
            None => {
                self.cur_doc_id = NO_MORE_DOCS;
                return None;
            }
        };

        self.find_common(lead_doc)
    }

    fn advance(&mut self, target: u32) -> Option<u32> {
        if self.children.is_empty() {
            return None;
        }

        let lead_doc = match self.children[0].advance(target) {
            Some(d) => d,
            None => {
                self.cur_doc_id = NO_MORE_DOCS;
                return None;
            }
        };

        self.find_common(lead_doc)
    }

    fn shallow_advance(&mut self, target: u32) -> Option<u32> {
        // For AND, shallow_advance all children and return the max last_doc_id
        // (since we need all to converge)
        let mut max_block_doc = 0u32;
        for child in &mut self.children {
            match child.shallow_advance(target) {
                Some(d) => max_block_doc = max_block_doc.max(d),
                None => return None,
            }
        }
        Some(max_block_doc)
    }

    fn doc_id(&self) -> u32 {
        self.cur_doc_id
    }

    fn block_max_doc(&self) -> u32 {
        // Min of children's block_max_doc — we can only go as far as the most constrained
        self.children
            .iter()
            .map(|c| c.block_max_doc())
            .min()
            .unwrap_or(NO_MORE_DOCS)
    }

    fn max_block_score(&self) -> f32 {
        // For AND, the max_block_score is the sum of all children's max_block_scores
        // (since all terms contribute to the final score for matching docs)
        self.children.iter().map(|c| c.max_block_score()).sum()
    }

    fn term_freq(&self) -> u32 {
        // Sum of term frequencies across all children
        self.children.iter().map(|c| c.term_freq()).sum()
    }

    fn doc_freq(&self) -> u32 {
        // Estimate: minimum of children's df (upper bound on intersection size)
        self.children
            .iter()
            .map(|c| c.doc_freq())
            .min()
            .unwrap_or(0)
    }
}

impl<'a> AndIterator<'a> {
    /// Find a doc_id that all iterators agree on, starting from `candidate`.
    fn find_common(&mut self, mut candidate: u32) -> Option<u32> {
        'outer: loop {
            for i in 1..self.children.len() {
                let doc = match self.children[i].advance(candidate) {
                    Some(d) => d,
                    None => {
                        self.cur_doc_id = NO_MORE_DOCS;
                        return None;
                    }
                };

                if doc != candidate {
                    // Mismatch — advance lead to this new candidate
                    candidate = match self.children[0].advance(doc) {
                        Some(d) => d,
                        None => {
                            self.cur_doc_id = NO_MORE_DOCS;
                            return None;
                        }
                    };
                    continue 'outer;
                }
            }
            // All agree on candidate
            self.cur_doc_id = candidate;
            return Some(candidate);
        }
    }
}

/// OR iterator: yields documents present in ANY child. Sums scores.
pub struct OrIterator<'a> {
    children: Vec<Box<dyn PostingIterator + 'a>>,
    /// Current doc_ids for each child.
    child_docs: Vec<u32>,
    cur_doc_id: u32,
    initialized: bool,
}

impl<'a> OrIterator<'a> {
    pub fn new(children: Vec<Box<dyn PostingIterator + 'a>>) -> Self {
        let n = children.len();
        Self {
            children,
            child_docs: vec![NO_MORE_DOCS; n],
            cur_doc_id: NO_MORE_DOCS,
            initialized: false,
        }
    }

    /// Initialize by advancing all children to their first doc.
    fn initialize(&mut self) {
        for i in 0..self.children.len() {
            self.child_docs[i] = self.children[i].next_doc().unwrap_or(NO_MORE_DOCS);
        }
        self.initialized = true;
    }
}

impl<'a> PostingIterator for OrIterator<'a> {
    fn next_doc(&mut self) -> Option<u32> {
        if !self.initialized {
            self.initialize();
        }

        // Find the minimum doc_id across all children
        let min_doc = *self.child_docs.iter().min().unwrap_or(&NO_MORE_DOCS);
        if min_doc == NO_MORE_DOCS {
            self.cur_doc_id = NO_MORE_DOCS;
            return None;
        }

        // Advance all children that are at min_doc to their next doc
        for i in 0..self.children.len() {
            if self.child_docs[i] == min_doc {
                self.child_docs[i] = self.children[i].next_doc().unwrap_or(NO_MORE_DOCS);
            }
        }

        self.cur_doc_id = min_doc;
        Some(min_doc)
    }

    fn advance(&mut self, target: u32) -> Option<u32> {
        if !self.initialized {
            self.initialize();
        }

        // If already at a doc >= target, return it
        if self.cur_doc_id != NO_MORE_DOCS && self.cur_doc_id >= target {
            return Some(self.cur_doc_id);
        }

        // Advance all children to >= target
        for i in 0..self.children.len() {
            if self.child_docs[i] < target && self.child_docs[i] != NO_MORE_DOCS {
                self.child_docs[i] = self.children[i].advance(target).unwrap_or(NO_MORE_DOCS);
            }
        }

        // Find the minimum doc_id across children
        let min_doc = *self.child_docs.iter().min().unwrap_or(&NO_MORE_DOCS);
        if min_doc == NO_MORE_DOCS {
            self.cur_doc_id = NO_MORE_DOCS;
            return None;
        }

        // Consume all children at min_doc (advance them to next)
        for i in 0..self.children.len() {
            if self.child_docs[i] == min_doc {
                self.child_docs[i] = self.children[i].next_doc().unwrap_or(NO_MORE_DOCS);
            }
        }

        self.cur_doc_id = min_doc;
        Some(min_doc)
    }

    fn shallow_advance(&mut self, target: u32) -> Option<u32> {
        // Shallow advance all children, return min block_max_doc
        let mut min_block = NO_MORE_DOCS;
        let mut any_valid = false;
        for child in &mut self.children {
            if let Some(d) = child.shallow_advance(target) {
                min_block = min_block.min(d);
                any_valid = true;
            }
        }
        if any_valid {
            Some(min_block)
        } else {
            None
        }
    }

    fn doc_id(&self) -> u32 {
        self.cur_doc_id
    }

    fn block_max_doc(&self) -> u32 {
        self.children
            .iter()
            .map(|c| c.block_max_doc())
            .min()
            .unwrap_or(NO_MORE_DOCS)
    }

    fn max_block_score(&self) -> f32 {
        // Sum of all children's max block scores (since any term can contribute)
        self.children.iter().map(|c| c.max_block_score()).sum()
    }

    fn term_freq(&self) -> u32 {
        // For OR, we don't have a single meaningful term_freq.
        // Return sum of children at current doc.
        // This is only called when doc is being scored, so children should be at correct pos.
        // Actually, for OR the scorer handles per-term scoring differently.
        // Return 0 as placeholder; BMW scorer uses individual term iterators.
        0
    }

    fn doc_freq(&self) -> u32 {
        // Upper bound: sum of children's df
        self.children.iter().map(|c| c.doc_freq()).sum()
    }
}

/// NOT iterator: yields docs from `positive` that are NOT in `negative`.
pub struct NotIterator<'a> {
    positive: Box<dyn PostingIterator + 'a>,
    negative: Box<dyn PostingIterator + 'a>,
    neg_doc: u32,
    cur_doc_id: u32,
    neg_initialized: bool,
}

impl<'a> NotIterator<'a> {
    pub fn new(positive: Box<dyn PostingIterator + 'a>, negative: Box<dyn PostingIterator + 'a>) -> Self {
        Self {
            positive,
            negative,
            neg_doc: NO_MORE_DOCS,
            cur_doc_id: NO_MORE_DOCS,
            neg_initialized: false,
        }
    }

    fn init_negative(&mut self) {
        if !self.neg_initialized {
            self.neg_doc = self.negative.next_doc().unwrap_or(NO_MORE_DOCS);
            self.neg_initialized = true;
        }
    }

    fn skip_negatives(&mut self, candidate: u32) -> bool {
        // Advance negative to >= candidate
        while self.neg_doc < candidate {
            self.neg_doc = self.negative.next_doc().unwrap_or(NO_MORE_DOCS);
        }
        // Returns true if candidate is NOT in negative
        self.neg_doc != candidate
    }
}

impl<'a> PostingIterator for NotIterator<'a> {
    fn next_doc(&mut self) -> Option<u32> {
        self.init_negative();
        loop {
            let doc = self.positive.next_doc()?;
            if self.skip_negatives(doc) {
                self.cur_doc_id = doc;
                return Some(doc);
            }
        }
    }

    fn advance(&mut self, target: u32) -> Option<u32> {
        self.init_negative();
        let mut doc = self.positive.advance(target)?;
        loop {
            if self.skip_negatives(doc) {
                self.cur_doc_id = doc;
                return Some(doc);
            }
            doc = self.positive.next_doc()?;
        }
    }

    fn shallow_advance(&mut self, target: u32) -> Option<u32> {
        self.positive.shallow_advance(target)
    }

    fn doc_id(&self) -> u32 {
        self.cur_doc_id
    }

    fn block_max_doc(&self) -> u32 {
        self.positive.block_max_doc()
    }

    fn max_block_score(&self) -> f32 {
        self.positive.max_block_score()
    }

    fn term_freq(&self) -> u32 {
        self.positive.term_freq()
    }

    fn doc_freq(&self) -> u32 {
        self.positive.doc_freq()
    }
}

// ---- Mock iterator for testing ----

#[cfg(test)]
pub struct MockIterator {
    postings: Vec<(u32, u32)>, // (doc_id, tf)
    pos: usize,
    df: u32,
    block_max: f32,
}

#[cfg(test)]
impl MockIterator {
    pub fn new(postings: Vec<(u32, u32)>) -> Self {
        let df = postings.len() as u32;
        Self {
            postings,
            pos: 0,
            df,
            block_max: 1.0,
        }
    }

    pub fn from_doc_ids(ids: &[u32]) -> Self {
        Self::new(ids.iter().map(|&id| (id, 1)).collect())
    }
}

#[cfg(test)]
impl PostingIterator for MockIterator {
    fn next_doc(&mut self) -> Option<u32> {
        if self.pos >= self.postings.len() {
            return None;
        }
        let (doc_id, _) = self.postings[self.pos];
        self.pos += 1;
        Some(doc_id)
    }

    fn advance(&mut self, target: u32) -> Option<u32> {
        // If we already consumed a doc >= target, return it without advancing
        if self.pos > 0 {
            let (last_doc, _) = self.postings[self.pos - 1];
            if last_doc >= target {
                return Some(last_doc);
            }
        }
        while self.pos < self.postings.len() {
            if self.postings[self.pos].0 >= target {
                let (doc_id, _) = self.postings[self.pos];
                self.pos += 1;
                return Some(doc_id);
            }
            self.pos += 1;
        }
        None
    }

    fn shallow_advance(&mut self, target: u32) -> Option<u32> {
        if !self.postings.is_empty() && self.postings.last().unwrap().0 >= target {
            Some(self.postings.last().unwrap().0)
        } else {
            None
        }
    }

    fn doc_id(&self) -> u32 {
        if self.pos == 0 {
            NO_MORE_DOCS
        } else {
            self.postings[self.pos - 1].0
        }
    }

    fn block_max_doc(&self) -> u32 {
        self.postings
            .last()
            .map(|&(id, _)| id)
            .unwrap_or(NO_MORE_DOCS)
    }

    fn max_block_score(&self) -> f32 {
        self.block_max
    }

    fn term_freq(&self) -> u32 {
        if self.pos == 0 {
            0
        } else {
            self.postings[self.pos - 1].1
        }
    }

    fn doc_freq(&self) -> u32 {
        self.df
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect_all(iter: &mut dyn PostingIterator) -> Vec<u32> {
        let mut result = Vec::new();
        while let Some(doc) = iter.next_doc() {
            result.push(doc);
        }
        result
    }

    // --- AND tests ---

    #[test]
    fn test_and_basic() {
        let a = Box::new(MockIterator::from_doc_ids(&[1, 2, 3, 5]));
        let b = Box::new(MockIterator::from_doc_ids(&[2, 3, 4, 5]));
        let mut and = AndIterator::new(vec![a, b]);
        assert_eq!(collect_all(&mut and), vec![2, 3, 5]);
    }

    #[test]
    fn test_and_no_overlap() {
        let a = Box::new(MockIterator::from_doc_ids(&[1, 3, 5]));
        let b = Box::new(MockIterator::from_doc_ids(&[2, 4, 6]));
        let mut and = AndIterator::new(vec![a, b]);
        let result: Vec<u32> = collect_all(&mut and);
        assert!(result.is_empty());
    }

    #[test]
    fn test_and_identical() {
        let a = Box::new(MockIterator::from_doc_ids(&[1, 2, 3]));
        let b = Box::new(MockIterator::from_doc_ids(&[1, 2, 3]));
        let mut and = AndIterator::new(vec![a, b]);
        assert_eq!(collect_all(&mut and), vec![1, 2, 3]);
    }

    #[test]
    fn test_and_three_way() {
        let a = Box::new(MockIterator::from_doc_ids(&[1, 2, 3, 4, 5]));
        let b = Box::new(MockIterator::from_doc_ids(&[2, 3, 5]));
        let c = Box::new(MockIterator::from_doc_ids(&[3, 5, 7]));
        let mut and = AndIterator::new(vec![a, b, c]);
        assert_eq!(collect_all(&mut and), vec![3, 5]);
    }

    #[test]
    fn test_and_advance() {
        let a = Box::new(MockIterator::from_doc_ids(&[1, 2, 3, 5, 8, 10]));
        let b = Box::new(MockIterator::from_doc_ids(&[2, 3, 5, 8, 10]));
        let mut and = AndIterator::new(vec![a, b]);
        assert_eq!(and.advance(4), Some(5));
        assert_eq!(and.advance(9), Some(10));
    }

    // --- OR tests ---

    #[test]
    fn test_or_basic() {
        let a = Box::new(MockIterator::from_doc_ids(&[1, 3, 5]));
        let b = Box::new(MockIterator::from_doc_ids(&[2, 3, 4]));
        let mut or = OrIterator::new(vec![a, b]);
        assert_eq!(collect_all(&mut or), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_or_no_overlap() {
        let a = Box::new(MockIterator::from_doc_ids(&[1, 3]));
        let b = Box::new(MockIterator::from_doc_ids(&[2, 4]));
        let mut or = OrIterator::new(vec![a, b]);
        assert_eq!(collect_all(&mut or), vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_or_identical() {
        let a = Box::new(MockIterator::from_doc_ids(&[1, 2, 3]));
        let b = Box::new(MockIterator::from_doc_ids(&[1, 2, 3]));
        let mut or = OrIterator::new(vec![a, b]);
        assert_eq!(collect_all(&mut or), vec![1, 2, 3]);
    }

    #[test]
    fn test_or_one_empty() {
        let a = Box::new(MockIterator::from_doc_ids(&[1, 2, 3]));
        let b = Box::new(MockIterator::from_doc_ids(&[]));
        let mut or = OrIterator::new(vec![a, b]);
        assert_eq!(collect_all(&mut or), vec![1, 2, 3]);
    }

    #[test]
    fn test_or_advance() {
        let a = Box::new(MockIterator::from_doc_ids(&[1, 3, 5, 7]));
        let b = Box::new(MockIterator::from_doc_ids(&[2, 4, 6, 8]));
        let mut or = OrIterator::new(vec![a, b]);
        assert_eq!(or.advance(3), Some(3));
        // After advance to 3, next should be 4
        assert_eq!(or.next_doc(), Some(4));
    }

    // --- NOT tests ---

    #[test]
    fn test_not_basic() {
        let pos = Box::new(MockIterator::from_doc_ids(&[1, 2, 3, 4, 5]));
        let neg = Box::new(MockIterator::from_doc_ids(&[2, 4]));
        let mut not = NotIterator::new(pos, neg);
        assert_eq!(collect_all(&mut not), vec![1, 3, 5]);
    }

    #[test]
    fn test_not_no_exclusions() {
        let pos = Box::new(MockIterator::from_doc_ids(&[1, 2, 3]));
        let neg = Box::new(MockIterator::from_doc_ids(&[10, 20]));
        let mut not = NotIterator::new(pos, neg);
        assert_eq!(collect_all(&mut not), vec![1, 2, 3]);
    }

    #[test]
    fn test_not_all_excluded() {
        let pos = Box::new(MockIterator::from_doc_ids(&[1, 2, 3]));
        let neg = Box::new(MockIterator::from_doc_ids(&[1, 2, 3]));
        let mut not = NotIterator::new(pos, neg);
        let result: Vec<u32> = collect_all(&mut not);
        assert!(result.is_empty());
    }

    #[test]
    fn test_not_advance() {
        let pos = Box::new(MockIterator::from_doc_ids(&[1, 2, 3, 4, 5, 6]));
        let neg = Box::new(MockIterator::from_doc_ids(&[2, 4, 6]));
        let mut not = NotIterator::new(pos, neg);
        assert_eq!(not.advance(3), Some(3));
        assert_eq!(not.next_doc(), Some(5));
    }

    // --- Composition tests ---

    #[test]
    fn test_and_or_composition() {
        // (A OR B) AND C
        let a = Box::new(MockIterator::from_doc_ids(&[1, 3, 5]));
        let b = Box::new(MockIterator::from_doc_ids(&[2, 3, 4]));
        let or: Box<dyn PostingIterator> = Box::new(OrIterator::new(vec![a, b]));
        let c = Box::new(MockIterator::from_doc_ids(&[3, 4, 5, 6]));
        let mut and = AndIterator::new(vec![or, c]);
        assert_eq!(collect_all(&mut and), vec![3, 4, 5]);
    }

    #[test]
    fn test_and_not_composition() {
        // A AND (NOT B)
        let a2 = Box::new(MockIterator::from_doc_ids(&[1, 2, 3, 4, 5]));
        let b2 = Box::new(MockIterator::from_doc_ids(&[2, 4]));
        let mut not2 = NotIterator::new(a2, b2);
        assert_eq!(collect_all(&mut not2), vec![1, 3, 5]);
    }
}
