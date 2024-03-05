use petgraph::graph::{DefaultIx, IndexType, NodeIndex};

#[cfg(test)]
mod tests;

/// An interval-value map, which support operations on dynamic sets of intervals.
#[derive(Debug)]
pub struct IntervalMap<T, V, Ix = DefaultIx> {
    /// Vector that stores nodes
    nodes: Vec<Node<T, V, Ix>>,
    /// Root of the interval tree
    root: NodeIndex<Ix>,
    /// Number of elements in the map
    len: usize,
}

impl<T, V, Ix> IntervalMap<T, V, Ix>
where
    T: Clone + Ord,
    Ix: IndexType,
{
    /// Creates a new `IntervalMap` with estimated capacity.
    #[inline]
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let mut nodes = vec![Self::new_sentinel()];
        nodes.reserve(capacity);
        IntervalMap {
            nodes,
            root: Self::sentinel(),
            len: 0,
        }
    }

    /// Inserts a interval-value pair into the map.
    ///
    /// # Panics
    ///
    /// This method panics when the tree is at the maximum number of nodes for its index
    #[inline]
    pub fn insert(&mut self, interval: Interval<T>, value: V) -> Option<V> {
        let node = Self::new_node(interval, value);
        let node_idx = NodeIndex::new(self.nodes.len());
        // check for max capacity, except if we use usize
        assert!(
            <Ix as IndexType>::max().index() == !0 || NodeIndex::end() != node_idx,
            "Reached maximum number of nodes"
        );
        self.nodes.push(node);
        self.insert_inner(node_idx)
    }

    /// Removes a interval from the map, returning the value at the interval if the interval
    /// was previously in the map.
    #[inline]
    pub fn remove(&mut self, interval: &Interval<T>) -> Option<V> {
        if let Some(node_idx) = self.search_exact(interval) {
            self.remove_inner(node_idx);
            // To achieve an O(1) time complexity for node removal, we swap the node
            // with the last node stored in the vector and update parent/left/right
            // nodes of the last node.
            let mut node = self.nodes.swap_remove(node_idx.index());
            let old = NodeIndex::<Ix>::new(self.nodes.len());
            self.update_idx(old, node_idx);

            return node.value.take();
        }
        None
    }

    /// Checks if an interval in the map overlaps with the given interval.
    #[inline]
    pub fn overlap(&self, interval: &Interval<T>) -> bool {
        let node_idx = self.search(interval);
        !self.nref(node_idx, Node::is_sentinel)
    }

    /// Finds all intervals in the map that overlaps with the given interval.
    #[inline]
    pub fn find_all_overlap(&self, interval: &Interval<T>) -> Vec<(&Interval<T>, &V)> {
        if self.nref(self.root, Node::is_sentinel) {
            Vec::new()
        } else {
            self.find_all_overlap_inner(self.root, interval)
        }
    }

    /// Returns a reference to the value corresponding to the key.
    #[inline]
    pub fn get(&self, interval: &Interval<T>) -> Option<&V> {
        self.search_exact(interval)
            .map(|idx| self.nref(idx, Node::value))
    }

    /// Returns a reference to the value corresponding to the key.
    #[inline]
    pub fn get_mut(&mut self, interval: &Interval<T>) -> Option<&mut V> {
        self.search_exact(interval)
            .map(|idx| self.nmut(idx, Node::value_mut))
    }

    /// Gets an iterator over the entries of the map, sorted by key.
    #[inline]
    #[must_use]
    pub fn iter(&self) -> Iter<'_, T, V, Ix> {
        Iter {
            map_ref: self,
            stack: None,
        }
    }

    /// Gets the given key's corresponding entry in the map for in-place manipulation.
    #[inline]
    pub fn entry(&mut self, interval: Interval<T>) -> Entry<'_, T, V, Ix> {
        match self.search_exact(&interval) {
            Some(node) => Entry::Occupied(OccupiedEntry {
                map_ref: self,
                node,
            }),
            None => Entry::Vacant(VacantEntry {
                map_ref: self,
                interval,
            }),
        }
    }

    /// Removes all elements from the map
    #[inline]
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.nodes.push(Self::new_sentinel());
        self.root = Self::sentinel();
        self.len = 0;
    }

    /// Returns the number of elements in the map.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the map contains no elements.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T, V> IntervalMap<T, V>
where
    T: Clone + Ord,
{
    /// Creates an empty `IntervalMap`
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self {
            nodes: vec![Self::new_sentinel()],
            root: Self::sentinel(),
            len: 0,
        }
    }
}

impl<T, V> Default for IntervalMap<T, V>
where
    T: Clone + Ord,
{
    #[inline]
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl<T, V, Ix> IntervalMap<T, V, Ix>
where
    T: Clone + Ord,
    Ix: IndexType,
{
    /// Creates a new sentinel node
    fn new_sentinel() -> Node<T, V, Ix> {
        Node {
            interval: None,
            value: None,
            max: None,
            left: None,
            right: None,
            parent: None,
            color: Color::Black,
        }
    }

    /// Creates a new tree node
    fn new_node(interval: Interval<T>, value: V) -> Node<T, V, Ix> {
        Node {
            max: Some(interval.high.clone()),
            interval: Some(interval),
            value: Some(value),
            left: Some(Self::sentinel()),
            right: Some(Self::sentinel()),
            parent: Some(Self::sentinel()),
            color: Color::Red,
        }
    }

    /// Gets the sentinel node index
    fn sentinel() -> NodeIndex<Ix> {
        NodeIndex::new(0)
    }
}

impl<T, V, Ix> IntervalMap<T, V, Ix>
where
    T: Ord + Clone,
    Ix: IndexType,
{
    /// Inserts a node into the tree.
    fn insert_inner(&mut self, z: NodeIndex<Ix>) -> Option<V> {
        let mut y = Self::sentinel();
        let mut x = self.root;

        while !self.nref(x, Node::is_sentinel) {
            y = x;
            if self.nref(z, Node::interval) == self.nref(y, Node::interval) {
                let zval = self.nmut(z, Node::take_value);
                let old_value = self.nmut(y, Node::set_value(zval));
                return Some(old_value);
            }
            if self.nref(z, Node::interval) < self.nref(x, Node::interval) {
                x = self.nref(x, Node::left);
            } else {
                x = self.nref(x, Node::right);
            }
        }
        self.nmut(z, Node::set_parent(y));
        if self.nref(y, Node::is_sentinel) {
            self.root = z;
        } else {
            if self.nref(z, Node::interval) < self.nref(y, Node::interval) {
                self.nmut(y, Node::set_left(z));
            } else {
                self.nmut(y, Node::set_right(z));
            }
            self.update_max_bottom_up(y);
        }
        self.nmut(z, Node::set_color(Color::Red));

        self.insert_fixup(z);

        self.len = self.len.wrapping_add(1);
        None
    }

    /// Removes a node from the tree.
    fn remove_inner(&mut self, z: NodeIndex<Ix>) {
        let mut y = z;
        let mut y_orig_color = self.nref(y, Node::color);
        let x;
        if self.left_ref(z, Node::is_sentinel) {
            x = self.nref(z, Node::right);
            self.transplant(z, x);
        } else if self.right_ref(z, Node::is_sentinel) {
            x = self.nref(z, Node::left);
            self.transplant(z, x);
        } else {
            y = self.tree_minimum(self.nref(z, Node::right));
            y_orig_color = self.nref(y, Node::color);
            x = self.nref(y, Node::right);
            if self.nref(y, Node::parent) == z {
                self.nmut(x, Node::set_parent(y));
            } else {
                self.transplant(y, x);
                self.nmut(y, Node::set_right(self.nref(z, Node::right)));
                self.right_mut(y, Node::set_parent(y));
            }
            self.transplant(z, y);
            self.nmut(y, Node::set_left(self.nref(z, Node::left)));
            self.left_mut(y, Node::set_parent(y));
            self.nmut(y, Node::set_color(self.nref(z, Node::color)));

            self.update_max_bottom_up(y);
        }

        if matches!(y_orig_color, Color::Black) {
            self.remove_fixup(x);
        }

        self.len = self.len.wrapping_sub(1);
    }

    /// Finds all intervals in the map that overlaps with the given interval.
    fn find_all_overlap_inner(
        &self,
        x: NodeIndex<Ix>,
        interval: &Interval<T>,
    ) -> Vec<(&Interval<T>, &V)> {
        let mut list = vec![];
        if self.nref(x, Node::interval).overlap(interval) {
            list.push(self.nref(x, |nx| (nx.interval(), nx.value())));
        }
        if self
            .left_ref(x, Node::sentinel)
            .map(Node::max)
            .is_some_and(|lm| lm >= &interval.low)
        {
            list.extend(self.find_all_overlap_inner(self.nref(x, Node::left), interval));
        }
        if self
            .right_ref(x, Node::sentinel)
            .map(|r| IntervalRef::new(&self.nref(x, Node::interval).low, r.max()))
            .is_some_and(|i| i.overlap(interval))
        {
            list.extend(self.find_all_overlap_inner(self.nref(x, Node::right), interval));
        }
        list
    }

    /// Search for an interval that overlaps with the given interval.
    fn search(&self, interval: &Interval<T>) -> NodeIndex<Ix> {
        let mut x = self.root;
        while self
            .nref(x, Node::sentinel)
            .map(Node::interval)
            .is_some_and(|xi| !xi.overlap(interval))
        {
            if self
                .left_ref(x, Node::sentinel)
                .map(Node::max)
                .is_some_and(|lm| lm > &interval.low)
            {
                x = self.nref(x, Node::left);
            } else {
                x = self.nref(x, Node::right);
            }
        }
        x
    }

    /// Search for the node with exact the given interval
    fn search_exact(&self, interval: &Interval<T>) -> Option<NodeIndex<Ix>> {
        let mut x = self.root;
        while !self.nref(x, Node::is_sentinel) {
            if self.nref(x, Node::interval) == interval {
                return Some(x);
            }
            if self.nref(x, Node::max) < &interval.high {
                return None;
            }
            if self.nref(x, Node::interval) > interval {
                x = self.nref(x, Node::left);
            } else {
                x = self.nref(x, Node::right);
            }
        }
        None
    }

    /// Restores red-black tree properties after an insert.
    fn insert_fixup(&mut self, mut z: NodeIndex<Ix>) {
        while self.parent_ref(z, Node::is_red) {
            if self.grand_parent_ref(z, Node::is_sentinel) {
                break;
            }
            if self.is_left_child(self.nref(z, Node::parent)) {
                let y = self.grand_parent_ref(z, Node::right);
                if self.nref(y, Node::is_red) {
                    self.parent_mut(z, Node::set_color(Color::Black));
                    self.nmut(y, Node::set_color(Color::Black));
                    self.grand_parent_mut(z, Node::set_color(Color::Red));
                    z = self.parent_ref(z, Node::parent);
                } else {
                    if self.is_right_child(z) {
                        z = self.nref(z, Node::parent);
                        self.left_rotate(z);
                    }
                    self.parent_mut(z, Node::set_color(Color::Black));
                    self.grand_parent_mut(z, Node::set_color(Color::Red));
                    self.right_rotate(self.parent_ref(z, Node::parent));
                }
            } else {
                let y = self.grand_parent_ref(z, Node::left);
                if self.nref(y, Node::is_red) {
                    self.parent_mut(z, Node::set_color(Color::Black));
                    self.nmut(y, Node::set_color(Color::Black));
                    self.grand_parent_mut(z, Node::set_color(Color::Red));
                    z = self.parent_ref(z, Node::parent);
                } else {
                    if self.is_left_child(z) {
                        z = self.nref(z, Node::parent);
                        self.right_rotate(z);
                    }
                    self.parent_mut(z, Node::set_color(Color::Black));
                    self.grand_parent_mut(z, Node::set_color(Color::Red));
                    self.left_rotate(self.parent_ref(z, Node::parent));
                }
            }
        }
        self.nmut(self.root, Node::set_color(Color::Black));
    }

    /// Restores red-black tree properties after a remove.
    fn remove_fixup(&mut self, mut x: NodeIndex<Ix>) {
        while x != self.root && self.nref(x, Node::is_black) {
            let mut w;
            if self.is_left_child(x) {
                w = self.parent_ref(x, Node::right);
                if self.nref(w, Node::is_red) {
                    self.nmut(w, Node::set_color(Color::Black));
                    self.parent_mut(x, Node::set_color(Color::Red));
                    self.left_rotate(self.nref(x, Node::parent));
                    w = self.parent_ref(x, Node::right);
                }
                if self.nref(w, Node::is_sentinel) {
                    break;
                }
                if self.left_ref(w, Node::is_black) && self.right_ref(w, Node::is_black) {
                    self.nmut(w, Node::set_color(Color::Red));
                    x = self.nref(x, Node::parent);
                } else {
                    if self.right_ref(w, Node::is_black) {
                        self.left_mut(w, Node::set_color(Color::Black));
                        self.nmut(w, Node::set_color(Color::Red));
                        self.right_rotate(w);
                        w = self.parent_ref(x, Node::right);
                    }
                    self.nmut(w, Node::set_color(self.parent_ref(x, Node::color)));
                    self.parent_mut(x, Node::set_color(Color::Black));
                    self.right_mut(w, Node::set_color(Color::Black));
                    self.left_rotate(self.nref(x, Node::parent));
                    x = self.root;
                }
            } else {
                w = self.parent_ref(x, Node::left);
                if self.nref(w, Node::is_red) {
                    self.nmut(w, Node::set_color(Color::Black));
                    self.parent_mut(x, Node::set_color(Color::Red));
                    self.right_rotate(self.nref(x, Node::parent));
                    w = self.parent_ref(x, Node::left);
                }
                if self.nref(w, Node::is_sentinel) {
                    break;
                }
                if self.right_ref(w, Node::is_black) && self.left_ref(w, Node::is_black) {
                    self.nmut(w, Node::set_color(Color::Red));
                    x = self.nref(x, Node::parent);
                } else {
                    if self.left_ref(w, Node::is_black) {
                        self.right_mut(w, Node::set_color(Color::Black));
                        self.nmut(w, Node::set_color(Color::Red));
                        self.left_rotate(w);
                        w = self.parent_ref(x, Node::left);
                    }
                    self.nmut(w, Node::set_color(self.parent_ref(x, Node::color)));
                    self.parent_mut(x, Node::set_color(Color::Black));
                    self.left_mut(w, Node::set_color(Color::Black));
                    self.right_rotate(self.nref(x, Node::parent));
                    x = self.root;
                }
            }
        }
        self.nmut(x, Node::set_color(Color::Black));
    }

    /// Binary tree left rotate.
    fn left_rotate(&mut self, x: NodeIndex<Ix>) {
        if self.right_ref(x, Node::is_sentinel) {
            return;
        }
        let y = self.nref(x, Node::right);
        self.nmut(x, Node::set_right(self.nref(y, Node::left)));
        if !self.left_ref(y, Node::is_sentinel) {
            self.left_mut(y, Node::set_parent(x));
        }

        self.replace_parent(x, y);
        self.nmut(y, Node::set_left(x));

        self.rotate_update_max(x, y);
    }

    /// Binary tree right rotate.
    fn right_rotate(&mut self, x: NodeIndex<Ix>) {
        if self.left_ref(x, Node::is_sentinel) {
            return;
        }
        let y = self.nref(x, Node::left);
        self.nmut(x, Node::set_left(self.nref(y, Node::right)));
        if !self.right_ref(y, Node::is_sentinel) {
            self.right_mut(y, Node::set_parent(x));
        }

        self.replace_parent(x, y);
        self.nmut(y, Node::set_right(x));

        self.rotate_update_max(x, y);
    }

    /// Replaces parent during a rotation.
    fn replace_parent(&mut self, x: NodeIndex<Ix>, y: NodeIndex<Ix>) {
        self.nmut(y, Node::set_parent(self.nref(x, Node::parent)));
        if self.parent_ref(x, Node::is_sentinel) {
            self.root = y;
        } else if self.is_left_child(x) {
            self.parent_mut(x, Node::set_left(y));
        } else {
            self.parent_mut(x, Node::set_right(y));
        }
        self.nmut(x, Node::set_parent(y));
    }

    /// Updates the max value after a rotation.
    fn rotate_update_max(&mut self, x: NodeIndex<Ix>, y: NodeIndex<Ix>) {
        self.nmut(y, Node::set_max(self.nref(x, Node::max_owned)));
        let mut max = &self.nref(x, Node::interval).high;
        if let Some(lmax) = self.left_ref(x, Node::sentinel).map(Node::max) {
            max = max.max(lmax);
        }
        if let Some(rmax) = self.right_ref(x, Node::sentinel).map(Node::max) {
            max = max.max(rmax);
        }
        self.nmut(x, Node::set_max(max.clone()));
    }

    /// Updates the max value towards the root
    fn update_max_bottom_up(&mut self, x: NodeIndex<Ix>) {
        let mut p = x;
        while !self.nref(p, Node::is_sentinel) {
            self.nmut(p, Node::set_max(self.nref(p, Node::interval).high.clone()));
            self.max_from(p, self.nref(p, Node::left));
            self.max_from(p, self.nref(p, Node::right));
            p = self.nref(p, Node::parent);
        }
    }

    /// Updates a nodes value from a child node.
    fn max_from(&mut self, x: NodeIndex<Ix>, c: NodeIndex<Ix>) {
        if let Some(cmax) = self.nref(c, Node::sentinel).map(Node::max) {
            let max = self.nref(x, Node::max).max(cmax).clone();
            self.nmut(x, Node::set_max(max));
        }
    }

    /// Finds the node with the minimum interval.
    fn tree_minimum(&self, mut x: NodeIndex<Ix>) -> NodeIndex<Ix> {
        while !self.left_ref(x, Node::is_sentinel) {
            x = self.nref(x, Node::left);
        }
        x
    }

    /// Replaces one subtree as a child of its parent with another subtree.
    fn transplant(&mut self, u: NodeIndex<Ix>, v: NodeIndex<Ix>) {
        if self.parent_ref(u, Node::is_sentinel) {
            self.root = v;
        } else {
            if self.is_left_child(u) {
                self.parent_mut(u, Node::set_left(v));
            } else {
                self.parent_mut(u, Node::set_right(v));
            }
            self.update_max_bottom_up(self.nref(u, Node::parent));
        }
        self.nmut(v, Node::set_parent(self.nref(u, Node::parent)));
    }

    /// Checks if a node is a left child of its parent.
    fn is_left_child(&self, node: NodeIndex<Ix>) -> bool {
        self.parent_ref(node, Node::left) == node
    }

    /// Checks if a node is a right child of its parent.
    fn is_right_child(&self, node: NodeIndex<Ix>) -> bool {
        self.parent_ref(node, Node::right) == node
    }

    /// Updates nodes index after remove
    fn update_idx(&mut self, old: NodeIndex<Ix>, new: NodeIndex<Ix>) {
        if self.root == old {
            self.root = new;
        }
        if self.nodes.get(new.index()).is_some() {
            if !self.parent_ref(new, Node::is_sentinel) {
                if self.parent_ref(new, Node::left) == old {
                    self.parent_mut(new, Node::set_left(new));
                } else {
                    self.parent_mut(new, Node::set_right(new));
                }
            }
            self.left_mut(new, Node::set_parent(new));
            self.right_mut(new, Node::set_parent(new));
        }
    }
}

// Convenient methods for reference or mutate current/parent/left/right node
#[allow(clippy::missing_docs_in_private_items)] // Trivial convenient methods
#[allow(clippy::indexing_slicing)] // Won't panic since all the indices we used are inbound
impl<'a, T, V, Ix> IntervalMap<T, V, Ix>
where
    Ix: IndexType,
{
    fn nref<F, R>(&'a self, node: NodeIndex<Ix>, op: F) -> R
    where
        R: 'a,
        F: FnOnce(&'a Node<T, V, Ix>) -> R,
    {
        op(&self.nodes[node.index()])
    }

    fn nmut<F, R>(&'a mut self, node: NodeIndex<Ix>, op: F) -> R
    where
        R: 'a,
        F: FnOnce(&'a mut Node<T, V, Ix>) -> R,
    {
        op(&mut self.nodes[node.index()])
    }

    fn left_ref<F, R>(&'a self, node: NodeIndex<Ix>, op: F) -> R
    where
        R: 'a,
        F: FnOnce(&'a Node<T, V, Ix>) -> R,
    {
        let idx = self.nodes[node.index()].left().index();
        op(&self.nodes[idx])
    }

    fn right_ref<F, R>(&'a self, node: NodeIndex<Ix>, op: F) -> R
    where
        R: 'a,
        F: FnOnce(&'a Node<T, V, Ix>) -> R,
    {
        let idx = self.nodes[node.index()].right().index();
        op(&self.nodes[idx])
    }

    fn parent_ref<F, R>(&'a self, node: NodeIndex<Ix>, op: F) -> R
    where
        R: 'a,
        F: FnOnce(&'a Node<T, V, Ix>) -> R,
    {
        let idx = self.nodes[node.index()].parent().index();
        op(&self.nodes[idx])
    }

    fn grand_parent_ref<F, R>(&'a self, node: NodeIndex<Ix>, op: F) -> R
    where
        R: 'a,
        F: FnOnce(&'a Node<T, V, Ix>) -> R,
    {
        let parent_idx = self.nodes[node.index()].parent().index();
        let grand_parent_idx = self.nodes[parent_idx].parent().index();
        op(&self.nodes[grand_parent_idx])
    }

    fn left_mut<F, R>(&'a mut self, node: NodeIndex<Ix>, op: F) -> R
    where
        R: 'a,
        F: FnOnce(&'a mut Node<T, V, Ix>) -> R,
    {
        let idx = self.nodes[node.index()].left().index();
        op(&mut self.nodes[idx])
    }

    fn right_mut<F, R>(&'a mut self, node: NodeIndex<Ix>, op: F) -> R
    where
        R: 'a,
        F: FnOnce(&'a mut Node<T, V, Ix>) -> R,
    {
        let idx = self.nodes[node.index()].right().index();
        op(&mut self.nodes[idx])
    }

    fn parent_mut<F, R>(&'a mut self, node: NodeIndex<Ix>, op: F) -> R
    where
        R: 'a,
        F: FnOnce(&'a mut Node<T, V, Ix>) -> R,
    {
        let idx = self.nodes[node.index()].parent().index();
        op(&mut self.nodes[idx])
    }

    fn grand_parent_mut<F, R>(&'a mut self, node: NodeIndex<Ix>, op: F) -> R
    where
        R: 'a,
        F: FnOnce(&'a mut Node<T, V, Ix>) -> R,
    {
        let parent_idx = self.nodes[node.index()].parent().index();
        let grand_parent_idx = self.nodes[parent_idx].parent().index();
        op(&mut self.nodes[grand_parent_idx])
    }
}

/// An iterator over the entries of a `IntervalMap`.
#[derive(Debug)]
pub struct Iter<'a, T, V, Ix> {
    /// Reference to the map
    map_ref: &'a IntervalMap<T, V, Ix>,
    /// Stack for iteration
    stack: Option<Vec<NodeIndex<Ix>>>,
}

impl<T, V, Ix> Iter<'_, T, V, Ix>
where
    Ix: IndexType,
{
    /// Initializes the stack
    fn init_stack(&mut self) {
        self.stack = Some(Self::left_tree(self.map_ref, self.map_ref.root));
    }

    /// Pushes x and x's left sub tree to stack.
    fn left_tree(map_ref: &IntervalMap<T, V, Ix>, mut x: NodeIndex<Ix>) -> Vec<NodeIndex<Ix>> {
        let mut nodes = vec![];
        while !map_ref.nref(x, Node::is_sentinel) {
            nodes.push(x);
            x = map_ref.nref(x, Node::left);
        }
        nodes
    }
}

impl<'a, T, V, Ix> Iterator for Iter<'a, T, V, Ix>
where
    Ix: IndexType,
{
    type Item = (&'a Interval<T>, &'a V);

    #[allow(clippy::unwrap_used, clippy::unwrap_in_result)]
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.stack.is_none() {
            self.init_stack();
        }
        let stack = self.stack.as_mut().unwrap();
        if stack.is_empty() {
            return None;
        }
        let x = stack.pop().unwrap();
        stack.extend(Self::left_tree(
            self.map_ref,
            self.map_ref.nref(x, Node::right),
        ));
        Some(self.map_ref.nref(x, |xn| (xn.interval(), xn.value())))
    }
}

/// A view into a single entry in a map, which may either be vacant or occupied.
#[allow(clippy::exhaustive_enums)] // It is final
#[derive(Debug)]
pub enum Entry<'a, T, V, Ix> {
    /// An occupied entry.
    Occupied(OccupiedEntry<'a, T, V, Ix>),
    /// A vacant entry.
    Vacant(VacantEntry<'a, T, V, Ix>),
}

/// A view into an occupied entry in a `IntervalMap`.
/// It is part of the [`Entry`] enum.
#[derive(Debug)]
pub struct OccupiedEntry<'a, T, V, Ix> {
    /// Reference to the map
    map_ref: &'a mut IntervalMap<T, V, Ix>,
    /// The entry node
    node: NodeIndex<Ix>,
}

/// A view into a vacant entry in a `IntervalMap`.
/// It is part of the [`Entry`] enum.
#[derive(Debug)]
pub struct VacantEntry<'a, T, V, Ix> {
    /// Mutable reference to the map
    map_ref: &'a mut IntervalMap<T, V, Ix>,
    /// The interval of this entry
    interval: Interval<T>,
}

impl<'a, T, V, Ix> Entry<'a, T, V, Ix>
where
    T: Ord + Clone,
    Ix: IndexType,
{
    /// Ensures a value is in the entry by inserting the default if empty, and returns
    /// a mutable reference to the value in the entry.
    #[inline]
    pub fn or_insert(self, default: V) -> &'a mut V {
        match self {
            Entry::Occupied(entry) => entry.map_ref.nmut(entry.node, Node::value_mut),
            Entry::Vacant(entry) => {
                let entry_idx = NodeIndex::new(entry.map_ref.nodes.len());
                let _ignore = entry.map_ref.insert(entry.interval, default);
                entry.map_ref.nmut(entry_idx, Node::value_mut)
            }
        }
    }

    /// Provides in-place mutable access to an occupied entry before any
    /// potential inserts into the map.
    ///
    /// # Panics
    ///
    /// This method panics when the node is a sentinel node
    #[inline]
    #[must_use]
    pub fn and_modify<F>(self, f: F) -> Self
    where
        F: FnOnce(&mut V),
    {
        match self {
            Entry::Occupied(entry) => {
                f(entry.map_ref.nmut(entry.node, Node::value_mut));
                Self::Occupied(entry)
            }
            Entry::Vacant(entry) => Self::Vacant(entry),
        }
    }
}

// TODO: better typed `Node`
/// Node of the interval tree
#[derive(Debug)]
pub struct Node<T, V, Ix> {
    /// Left children
    left: Option<NodeIndex<Ix>>,
    /// Right children
    right: Option<NodeIndex<Ix>>,
    /// Parent
    parent: Option<NodeIndex<Ix>>,
    /// Color of the node
    color: Color,

    /// Interval of the node
    interval: Option<Interval<T>>,
    /// Max value of the sub-tree of the node
    max: Option<T>,
    /// Value of the node
    value: Option<V>,
}

// Convenient getter/setter methods
#[allow(clippy::missing_docs_in_private_items)]
#[allow(clippy::missing_docs_in_private_items)] // Trivial convenient methods
#[allow(clippy::unwrap_used)] // Won't panic since the conditions are checked in the implementation
impl<T, V, Ix> Node<T, V, Ix>
where
    Ix: IndexType,
{
    fn color(&self) -> Color {
        self.color
    }

    fn interval(&self) -> &Interval<T> {
        self.interval.as_ref().unwrap()
    }

    fn max(&self) -> &T {
        self.max.as_ref().unwrap()
    }

    fn max_owned(&self) -> T
    where
        T: Clone,
    {
        self.max().clone()
    }

    fn left(&self) -> NodeIndex<Ix> {
        self.left.unwrap()
    }

    fn right(&self) -> NodeIndex<Ix> {
        self.right.unwrap()
    }

    fn parent(&self) -> NodeIndex<Ix> {
        self.parent.unwrap()
    }

    fn is_sentinel(&self) -> bool {
        self.interval.is_none()
    }

    fn sentinel(&self) -> Option<&Self> {
        self.interval.is_some().then_some(self)
    }

    fn is_black(&self) -> bool {
        matches!(self.color, Color::Black)
    }

    fn is_red(&self) -> bool {
        matches!(self.color, Color::Red)
    }

    fn value(&self) -> &V {
        self.value.as_ref().unwrap()
    }

    fn value_mut(&mut self) -> &mut V {
        self.value.as_mut().unwrap()
    }

    fn take_value(&mut self) -> V {
        self.value.take().unwrap()
    }

    fn set_value(value: V) -> impl FnOnce(&mut Node<T, V, Ix>) -> V {
        move |node: &mut Node<T, V, Ix>| node.value.replace(value).unwrap()
    }

    fn set_color(color: Color) -> impl FnOnce(&mut Node<T, V, Ix>) {
        move |node: &mut Node<T, V, Ix>| {
            node.color = color;
        }
    }

    fn set_max(max: T) -> impl FnOnce(&mut Node<T, V, Ix>) {
        move |node: &mut Node<T, V, Ix>| {
            let _ignore = node.max.replace(max);
        }
    }

    fn set_left(left: NodeIndex<Ix>) -> impl FnOnce(&mut Node<T, V, Ix>) {
        move |node: &mut Node<T, V, Ix>| {
            let _ignore = node.left.replace(left);
        }
    }

    fn set_right(right: NodeIndex<Ix>) -> impl FnOnce(&mut Node<T, V, Ix>) {
        move |node: &mut Node<T, V, Ix>| {
            let _ignore = node.right.replace(right);
        }
    }

    fn set_parent(parent: NodeIndex<Ix>) -> impl FnOnce(&mut Node<T, V, Ix>) {
        move |node: &mut Node<T, V, Ix>| {
            let _ignore = node.parent.replace(parent);
        }
    }
}

/// The Interval stored in `IntervalMap`
/// Represents the interval [low, high)
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Interval<T> {
    /// Low value
    low: T,
    /// high value
    high: T,
}

impl<T: Ord> Interval<T> {
    /// Creates a new `Interval`
    ///
    /// # Panics
    ///
    /// This method panics when low is greater than high
    #[inline]
    pub fn new(low: T, high: T) -> Self {
        assert!(low < high, "invalid range");
        Self { low, high }
    }

    /// Checks if self overlaps with other interval
    #[inline]
    pub fn overlap(&self, other: &Self) -> bool {
        self.high > other.low && other.high > self.low
    }
}

/// Reference type of `Interval`
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct IntervalRef<'a, T> {
    /// Low value
    low: &'a T,
    /// high value
    high: &'a T,
}

impl<'a, T: Ord> IntervalRef<'a, T> {
    /// Creates a new `IntervalRef`
    ///
    /// # Panics
    ///
    /// This method panics when low is greater than high
    #[inline]
    fn new(low: &'a T, high: &'a T) -> Self {
        assert!(low < high, "invalid range");
        Self { low, high }
    }

    /// Checks if self overlaps with a `Interval<T>`
    fn overlap(&self, other: &Interval<T>) -> bool {
        self.high > &other.low && &other.high > self.low
    }
}

/// The color of the node
#[derive(Debug, Clone, Copy)]
enum Color {
    /// Red node
    Red,
    /// Black node
    Black,
}
