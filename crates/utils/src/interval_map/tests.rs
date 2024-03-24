use std::collections::HashSet;

use rand::{rngs::StdRng, Rng, SeedableRng};

use super::*;

struct IntervalGenerator {
    rng: StdRng,
    unique: HashSet<Interval<i32>>,
    limit: i32,
}

impl IntervalGenerator {
    fn new(seed: [u8; 32]) -> Self {
        const LIMIT: i32 = 1000;
        Self {
            rng: SeedableRng::from_seed(seed),
            unique: HashSet::new(),
            limit: LIMIT,
        }
    }

    fn next(&mut self) -> Interval<i32> {
        let low = self.rng.gen_range(0..self.limit - 1);
        let high = self.rng.gen_range((low + 1)..self.limit);
        Interval::new(low, high)
    }

    fn next_unique(&mut self) -> Interval<i32> {
        let mut interval = self.next();
        while self.unique.contains(&interval) {
            interval = self.next();
        }
        self.unique.insert(interval.clone());
        interval
    }

    fn next_with_range(&mut self, range: i32) -> Interval<i32> {
        let low = self.rng.gen_range(0..self.limit - 1);
        let high = self
            .rng
            .gen_range((low + 1)..self.limit.min(low + 1 + range));
        Interval::new(low, high)
    }
}

impl<V> IntervalMap<i32, V> {
    fn check_max(&self) {
        let _ignore = self.check_max_inner(self.root);
    }

    fn check_max_inner(&self, x: NodeIndex<u32>) -> i32 {
        if self.node_ref(x, Node::is_sentinel) {
            return 0;
        }
        let l_max = self.check_max_inner(self.node_ref(x, Node::left));
        let r_max = self.check_max_inner(self.node_ref(x, Node::right));
        let max = self.node_ref(x, |x| x.interval().high.max(l_max).max(r_max));
        assert_eq!(self.max(x), Some(&max));
        max
    }

    /// 1. Every node is either red or black.
    /// 2. The root is black.
    /// 3. Every leaf (NIL) is black.
    /// 4. If a node is red, then both its children are black.
    /// 5. For each node, all simple paths from the node to descendant leaves contain the
    /// same number of black nodes.
    fn check_rb_properties(&self) {
        assert!(matches!(
            self.node_ref(self.root, Node::color),
            Color::Black
        ));
        self.check_children_color(self.root);
        self.check_black_height(self.root);
    }

    fn check_children_color(&self, x: NodeIndex<u32>) {
        if self.node_ref(x, Node::is_sentinel) {
            return;
        }
        self.check_children_color(self.node_ref(x, Node::left));
        self.check_children_color(self.node_ref(x, Node::right));
        if self.node_ref(x, Node::is_red) {
            assert!(matches!(self.left_ref(x, Node::color), Color::Black));
            assert!(matches!(self.right_ref(x, Node::color), Color::Black));
        }
    }

    fn check_black_height(&self, x: NodeIndex<u32>) -> usize {
        if self.node_ref(x, Node::is_sentinel) {
            return 0;
        }
        let lefth = self.check_black_height(self.node_ref(x, Node::left));
        let righth = self.check_black_height(self.node_ref(x, Node::right));
        assert_eq!(lefth, righth);
        if self.node_ref(x, Node::is_black) {
            return lefth + 1;
        }
        lefth
    }
}

fn with_map_and_generator<V>(test_fn: impl Fn(IntervalMap<i32, V>, IntervalGenerator)) {
    let seeds = vec![[0; 32], [1; 32], [2; 32]];
    for seed in seeds {
        let gen = IntervalGenerator::new(seed);
        let map = IntervalMap::new();
        test_fn(map, gen);
    }
}

#[test]
fn red_black_tree_properties_is_satisfied() {
    with_map_and_generator(|mut map, mut gen| {
        let intervals: Vec<_> = std::iter::repeat_with(|| gen.next_unique())
            .take(1000)
            .collect();
        for i in intervals.clone() {
            let _ignore = map.insert(i, ());
        }
        map.check_rb_properties();
    });
}

#[test]
#[should_panic]
fn invalid_range_should_panic() {
    let _interval = Interval::new(3, 1);
}

#[test]
fn insert_equal_interval_returns_previous_value() {
    let mut map = IntervalMap::new();
    map.insert(Interval::new(1, 3), 1);
    assert_eq!(map.insert(Interval::new(1, 3), 2), Some(1));
    assert_eq!(map.insert(Interval::new(1, 3), 3), Some(2));
}

#[test]
fn map_len_will_update() {
    with_map_and_generator(|mut map, mut gen| {
        let intervals: Vec<_> = std::iter::repeat_with(|| gen.next_unique())
            .take(100)
            .collect();
        for i in intervals.clone() {
            let _ignore = map.insert(i, ());
        }
        assert_eq!(map.len(), 100);
        for i in intervals {
            let _ignore = map.remove(&i);
        }
        assert_eq!(map.len(), 0);
    });
}

#[test]
fn check_overlap_is_ok_simple() {
    let mut map = IntervalMap::new();
    map.insert(Interval::new(1, 3), ());
    map.insert(Interval::new(6, 7), ());
    map.insert(Interval::new(9, 11), ());
    assert!(map.overlap(&Interval::new(2, 5)));
    assert!(map.overlap(&Interval::new(1, 17)));
    assert!(!map.overlap(&Interval::new(4, 5)));
    assert!(!map.overlap(&Interval::new(20, 23)));
}

#[test]
fn check_overlap_is_ok() {
    with_map_and_generator(|mut map, mut gen| {
        let intervals: Vec<_> = std::iter::repeat_with(|| gen.next_with_range(10))
            .take(100)
            .collect();
        for i in intervals.clone() {
            let _ignore = map.insert(i, ());
        }
        let to_check: Vec<_> = std::iter::repeat_with(|| gen.next_with_range(10))
            .take(1000)
            .collect();
        let expects: Vec<_> = to_check
            .iter()
            .map(|ci| intervals.iter().any(|i| ci.overlap(i)))
            .collect();

        for (ci, expect) in to_check.into_iter().zip(expects.into_iter()) {
            assert_eq!(map.overlap(&ci), expect);
        }
    });
}

#[test]
fn check_max_is_ok() {
    with_map_and_generator(|mut map, mut gen| {
        let intervals: Vec<_> = std::iter::repeat_with(|| gen.next_unique())
            .take(1000)
            .collect();
        for i in intervals.clone() {
            let _ignore = map.insert(i, ());
            map.check_max();
        }
        assert_eq!(map.len(), 1000);
        for i in intervals {
            let _ignore = map.remove(&i);
            map.check_max();
        }
    });
}

#[test]
fn remove_non_exist_interval_will_do_nothing() {
    with_map_and_generator(|mut map, mut gen| {
        let intervals: Vec<_> = std::iter::repeat_with(|| gen.next_unique())
            .take(1000)
            .collect();
        for i in intervals {
            let _ignore = map.insert(i, ());
        }
        assert_eq!(map.len(), 1000);
        let to_remove: Vec<_> = std::iter::repeat_with(|| gen.next_unique())
            .take(1000)
            .collect();
        for i in to_remove {
            let _ignore = map.remove(&i);
        }
        assert_eq!(map.len(), 1000);
    });
}

#[test]
fn find_all_overlap_is_ok_simple() {
    let mut map = IntervalMap::new();
    map.insert(Interval::new(1, 3), ());
    map.insert(Interval::new(2, 4), ());
    map.insert(Interval::new(6, 7), ());
    map.insert(Interval::new(7, 11), ());
    assert_eq!(map.find_all_overlap(&Interval::new(2, 7)).len(), 3);
    map.remove(&Interval::new(1, 3));
    assert_eq!(map.find_all_overlap(&Interval::new(2, 7)).len(), 2);
}

#[test]
fn find_all_overlap_is_ok() {
    with_map_and_generator(|mut map, mut gen| {
        let intervals: Vec<_> = std::iter::repeat_with(|| gen.next_unique())
            .take(1000)
            .collect();
        for i in intervals.clone() {
            let _ignore = map.insert(i, ());
        }
        let to_find: Vec<_> = std::iter::repeat_with(|| gen.next()).take(1000).collect();

        let expects: Vec<Vec<_>> = to_find
            .iter()
            .map(|ti| intervals.iter().filter(|i| ti.overlap(i)).collect())
            .collect();

        for (ti, mut expect) in to_find.into_iter().zip(expects.into_iter()) {
            let mut result = map.find_all_overlap(&ti);
            expect.sort_unstable();
            result.sort_unstable();
            assert_eq!(expect.len(), result.len());
            for (e, r) in expect.into_iter().zip(result.into_iter()) {
                assert_eq!(e, r.0);
            }
        }
    });
}

#[test]
fn entry_modify_is_ok() {
    let mut map = IntervalMap::new();
    map.insert(Interval::new(1, 3), 1);
    map.insert(Interval::new(2, 4), 2);
    map.insert(Interval::new(6, 7), 3);
    map.insert(Interval::new(7, 11), 4);
    let _ignore = map.entry(Interval::new(6, 7)).and_modify(|v| *v += 1);
    assert_eq!(map.get(&Interval::new(1, 3)), Some(&1));
    assert_eq!(map.get(&Interval::new(2, 4)), Some(&2));
    assert_eq!(map.get(&Interval::new(6, 7)), Some(&4));
    assert_eq!(map.get(&Interval::new(7, 11)), Some(&4));
    assert_eq!(map.get(&Interval::new(5, 17)), None);
    map.entry(Interval::new(3, 5))
        .and_modify(|v| *v += 1)
        .or_insert(0);
    let _ignore = map.get_mut(&Interval::new(3, 5)).map(|v| *v += 1);
    assert_eq!(map.get(&Interval::new(3, 5)), Some(&1));
}

#[test]
fn iterate_through_map_is_sorted() {
    with_map_and_generator(|mut map, mut gen| {
        let mut intervals: Vec<_> = std::iter::repeat_with(|| gen.next_unique())
            .enumerate()
            .take(1000)
            .collect();
        for (v, i) in intervals.clone() {
            let _ignore = map.insert(i, v);
        }
        intervals.sort_unstable_by(|a, b| a.1.cmp(&b.1));

        #[allow(clippy::pattern_type_mismatch)]
        for ((ei, ev), (v, i)) in map.iter().zip(intervals.iter()) {
            assert_eq!(ei, i);
            assert_eq!(ev, v);
        }
    });
}

#[test]
fn interval_map_clear_is_ok() {
    let mut map = IntervalMap::new();
    map.insert(Interval::new(1, 3), 1);
    map.insert(Interval::new(2, 4), 2);
    map.insert(Interval::new(6, 7), 3);
    assert_eq!(map.len(), 3);
    map.clear();
    assert_eq!(map.len(), 0);
    assert!(map.is_empty());
    assert_eq!(map.nodes.len(), 1);
    assert!(map.nodes[0].is_sentinel());
}
