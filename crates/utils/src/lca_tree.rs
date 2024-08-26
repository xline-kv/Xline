use std::ops::{Add, Sub as _};

/// A LCA tree to accelerate Txns' key overlap validation
#[non_exhaustive]
#[derive(Debug)]
pub struct LCATree {
    ///
    nodes: Vec<LCANode>,
}

///
#[non_exhaustive]
#[derive(Debug)]
pub struct LCANode {
    ///
    pub parent: Vec<usize>,
    ///
    pub depth: usize,
}

#[allow(clippy::indexing_slicing)]
impl LCATree {
    /// build a `LCATree` with a sentinel node
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: vec![LCANode {
                parent: vec![0],
                depth: 0,
            }],
        }
    }
    /// get a node by index
    ///
    /// # Panics
    ///
    /// The function panics if given `i` > max index
    #[inline]
    #[must_use]
    pub fn get_node(&self, i: usize) -> &LCANode {
        assert!(i < self.nodes.len(), "Node {i} doesn't exist");
        &self.nodes[i]
    }
    /// insert a node and return its index
    ///
    /// # Panics
    ///
    /// The function panics if given `parent` doesn't exist
    #[inline]
    #[must_use]
    #[allow(clippy::as_conversions)]
    pub fn insert_node(&mut self, parent: usize) -> usize {
        let depth = if parent == 0 {
            0
        } else {
            self.get_node(parent).depth.add(1)
        };
        let mut node = LCANode {
            parent: vec![],
            depth,
        };
        node.parent.push(parent);
        let parent_num = if depth == 0 { 0 } else { depth.ilog2() } as usize;
        for i in 0..parent_num {
            node.parent.push(self.get_node(node.parent[i]).parent[i]);
        }
        self.nodes.push(node);
        self.nodes.len().sub(1)
    }
    /// Use Binary Lifting to find the LCA of `node_a` and `node_b`
    ///
    /// # Panics
    ///
    /// The function panics if given `node_a` or `node_b` doesn't exist
    #[inline]
    #[must_use]
    pub fn find_lca(&self, node_a: usize, node_b: usize) -> usize {
        let (mut x, mut y) = if self.get_node(node_a).depth < self.get_node(node_b).depth {
            (node_a, node_b)
        } else {
            (node_b, node_a)
        };
        while self.get_node(x).depth < self.get_node(y).depth {
            for ancestor in self.get_node(y).parent.iter().rev() {
                if self.get_node(x).depth <= self.get_node(*ancestor).depth {
                    y = *ancestor;
                }
            }
        }
        while x != y {
            let node_x = self.get_node(x);
            let node_y = self.get_node(y);
            if node_x.parent[0] == node_y.parent[0] {
                x = node_x.parent[0];
                break;
            }
            for i in (0..node_x.parent.len()).rev() {
                if node_x.parent[i] != node_y.parent[i] {
                    x = node_x.parent[i];
                    y = node_y.parent[i];
                    break;
                }
            }
        }
        x
    }
}

impl Default for LCATree {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use crate::lca_tree::LCATree;

    #[test]
    fn test_ilog2() {
        assert_eq!(3_i32.ilog2(), 1);
        assert_eq!(5_i32.ilog2(), 2);
        assert_eq!(7_i32.ilog2(), 2);
        assert_eq!(10_i32.ilog2(), 3);
    }

    #[test]
    //              root
    //         /     |    \
    //        /      |     \
    //       /       |      \
    //  node1        node2  node3
    //  |    \       |      |
    //  |     \      |      |
    //  node4  node5 node6  node7
    //  |  \     \
    //  |   \    node10
    //  node8 node9
    //
    //
    fn test_lca() {
        let mut tree = LCATree::new();
        let root = 0;
        let node1 = tree.insert_node(root);
        let node2 = tree.insert_node(root);
        let node3 = tree.insert_node(root);

        let node4 = tree.insert_node(node1);
        let node5 = tree.insert_node(node1);

        let node6 = tree.insert_node(node2);

        let node7 = tree.insert_node(node3);

        let node8 = tree.insert_node(node4);
        let node9 = tree.insert_node(node4);

        let node10 = tree.insert_node(node5);

        assert_eq!(tree.find_lca(node1, node2), root);
        assert_eq!(tree.find_lca(node1, node3), root);
        assert_eq!(tree.find_lca(node1, node4), node1);
        assert_eq!(tree.find_lca(node4, node5), node1);
        assert_eq!(tree.find_lca(node5, node7), root);
        assert_eq!(tree.find_lca(node6, node7), root);
        assert_eq!(tree.find_lca(node8, node9), node4);
        assert_eq!(tree.find_lca(node8, node10), node1);
        assert_eq!(tree.find_lca(node6, node10), root);
        assert_eq!(tree.find_lca(node8, node5), node1);
        assert_eq!(tree.find_lca(node9, node3), root);
        assert_eq!(tree.find_lca(node10, node2), root);
    }
}
