// nodes: HashMap<u64, Node>,
// groups: HashMap<u64, RaftGroup<RS>>,

use std::collections::HashMap;
use std::collections::hash_map::Iter;

use prost::encoding::group;

/// Node represents a physical node and contains a group of rafts.
pub struct Node {
    pub node_id: u64,
    pub group_map: HashMap<u64, ()>,
}

pub struct NodeManager {
    pub nodes: HashMap<u64, Node>,
}

impl NodeManager {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    #[inline]
    pub fn iter(&self) -> Iter<'_, u64, Node> {
       self.nodes.iter()
    }

    #[inline]
    pub fn contains_node(&self, node_id: &u64) -> bool {
        self.nodes.contains_key(node_id)
    }

    #[inline]
    pub fn get_node(&self, node_id: &u64) -> Option<&Node> {
        self.nodes.get(node_id)
    }

    pub fn add_node(&mut self, node_id: u64, group_id: u64) {
        let node = match self.nodes.get_mut(&node_id) {
            None => {
                self.nodes.insert(
                    node_id,
                    Node {
                        node_id,
                        group_map: HashMap::new(),
                    },
                );
                self.nodes.get_mut(&node_id).unwrap()
            }
            Some(node) => node,
        };

        // assert_ne!(group_id, 0);
        node.group_map.insert(group_id, ());
    }
}
