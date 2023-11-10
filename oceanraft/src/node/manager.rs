use std::collections::hash_map::HashMap;
use std::collections::hash_map::Iter;

/// Node represents a single node identify in the cluster.
#[derive(Debug)]
pub struct Node {
    pub node_id: u64,
    pub groups: HashMap<u64, ()>,
}

/// NodeManager manages all the nodes in the cluster.
pub struct NodeManager {
    nodes: HashMap<u64, Node>,
}

impl NodeManager {
    /// Create a new node manager.
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    /// Returns an iterator over the nodes.
    #[inline]
    pub fn iter(&self) -> Iter<'_, u64, Node> {
        self.nodes.iter()
    }

    /// Returns true if the node manager contains the node.
    #[inline]
    pub fn contains_node(&self, node_id: &u64) -> bool {
        self.nodes.contains_key(node_id)
    }

    /// Returns the node with the given node id.
    #[inline]
    pub fn get_node(&self, node_id: &u64) -> Option<&Node> {
        self.nodes.get(node_id)
    }

    /// Add node to the node manager. If the node already exists, do nothing.
    /// Otherwise, add the node to the node manager.
    pub fn add_node(&mut self, node_id: u64) {
        if self.nodes.get_mut(&node_id).is_none() {
            self.nodes.insert(
                node_id,
                Node {
                    node_id,
                    groups: HashMap::new(),
                },
            );
        }
    }

    pub(crate) fn add_group(&mut self, node_id: u64, group_id: u64) {
        let node = match self.nodes.get_mut(&node_id) {
            None => self.nodes.entry(node_id).or_insert(Node {
                node_id,
                groups: HashMap::new(),
            }),
            Some(node) => node,
        };

        assert_ne!(group_id, 0);
        node.groups.insert(group_id, ());
    }

    pub(crate) fn remove_group(&mut self, node_id: u64, group_id: u64) {
        let node = match self.nodes.get_mut(&node_id) {
            None => return,
            Some(node) => node,
        };

        node.groups.remove(&group_id);
    }
}
