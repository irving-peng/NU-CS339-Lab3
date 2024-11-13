use crate::common::constants::INF;
use crate::storage::buffer::buffer_pool_manager::FrameId;
use std::collections::{HashMap, VecDeque};

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum AccessType {
    Unknown = 0,
    Lookup,
    Scan,
    Index,
}

#[derive(Debug)]
pub struct LRUKNode {
    /// History of last seen k timestamps of this page. Least recent timestamp stored in front.
    pub(crate) history: VecDeque<usize>,
    pub(crate) k: usize,
    pub(crate) is_evictable: bool,
}

impl LRUKNode {
    fn new(k: usize) -> Self {
        Self {
            history: VecDeque::with_capacity(k),
            k,
            is_evictable: false,
        }
    }

    /// # Returns
    /// - the k'th most recent timestamp's distance from the current timestamp if k accesses
    ///   have been recorded, and `usize::MAX` otherwise
    pub(crate) fn get_backwards_k_distance(&self, current_timestamp: usize) -> usize {
        ////////////////////////////// Begin: Students Implement  //////////////////////////////

        if self.has_infinite_backwards_k_distance() {
            return INF;
        }
        current_timestamp - self.get_kth_most_recent_timestamp()

        ////////////////////////////// End: Students Implement  //////////////////////////////
    }

    pub(crate) fn has_infinite_backwards_k_distance(&self) -> bool {
        ////////////////////////////// Begin: Students Implement  //////////////////////////////

        self.history.len() < self.k

        ////////////////////////////// End: Students Implement  //////////////////////////////
    }

    ////////////////////////////// Begin: Not Visible to Students //////////////////////////////

    fn get_kth_most_recent_timestamp(&self) -> usize {
        let number_of_accesses = self.history.len();
        if number_of_accesses < self.k {
            panic!("Node has {number_of_accesses} < `k` accesses in its history.");
        }
        *self.history.front().unwrap()
    }

    fn get_most_recent_timestamp(&self) -> usize {
        *self
            .history
            .back()
            .expect("Node does not have any timestamps in its history.")
    }

    ////////////////////////////// End: Not Visible to Students //////////////////////////////
}

#[derive(Debug)]
pub struct LRUKReplacer {
    pub(crate) node_store: HashMap<FrameId, LRUKNode>,
    pub(crate) current_timestamp: usize,
    // Number of evictable frames in the replacer. Note: this might not be the size of `node_store`!
    pub(crate) curr_size: usize,
    // Maximum number of frames that can be stored in the replacer.
    pub(crate) max_size: usize,
    pub(crate) k: usize,
}

impl LRUKReplacer {
    pub fn new(num_frames: usize, k: usize) -> Self {
        Self {
            node_store: HashMap::new(),
            current_timestamp: 0,
            curr_size: 0,
            max_size: num_frames,
            k,
        }
    }

    pub fn builder() -> LRUKReplacerBuilder {
        LRUKReplacerBuilder {
            node_store: HashMap::new(),
            current_timestamp: 0,
            curr_size: 0,
            max_size: None,
            k: None,
        }
    }

    /// Evict the frame with the largest backwards k-distance. If a frame has
    /// not been accessed k times, its backwards k-distance is considered to
    /// be infinite. If there are multiple frames with infinite k-distance,
    /// choose the one to evict based on LRU.
    ///
    /// # Returns
    /// - an Option that is either `Some(frame_id)` if a frame with id `frame_id` was evicted, and
    ///   `None` otherwise
    pub fn evict(&mut self) -> Option<FrameId> {
        ////////////////////////////// Begin: Students Implement  //////////////////////////////

        let frame_id = self.get_frame_to_evict()?;
        self.evict_frame(&frame_id);
        self.decrement_current_size();

        Some(frame_id)

        ////////////////////////////// End: Students Implement  //////////////////////////////
    }

    /// Record an access to a frame at the current timestamp.
    ///
    /// This method should update the k-history of the frame and increment the current timestamp.
    /// If the given `frame_id` is invalid (i.e. >= `max_size`), this method throws an exception.
    ///
    /// # Parameters
    /// - `frame_id`: The id of the frame that was accessed
    /// - `access_type`: The type of access that occurred (e.g., Lookup, Scan, Index)
    pub fn record_access(&mut self, frame_id: &FrameId, _access_type: AccessType) {
        ////////////////////////////// Begin: Students Implement  //////////////////////////////

        if *frame_id >= self.max_size {
            panic!(
                "FrameId {frame_id} is invalid (replacer size: {})",
                self.max_size
            );
        }

        if !self.node_store.contains_key(frame_id) && self.curr_size < self.max_size {
            let node = LRUKNode::new(self.k);
            self.node_store.insert(*frame_id, node);
        }
        self.modify_node_history(frame_id);

        ////////////////////////////// End: Students Implement  //////////////////////////////
    }

    /// Set the evictable status of a frame. Note that replacer's curr_size is equal
    /// to the number of evictable frames.
    ///
    /// If a frame was previously evictable and is set to be non-evictable,
    /// then curr_size should decrement. If a frame was previously non-evictable and
    /// is to be set to evictable, then curr_size should increment. If the frame id is
    /// invalid, throw an exception or abort the process.
    ///
    /// For other scenarios, this function should terminate without modifying anything.
    ///
    /// # Parameters
    /// - `frame_id`: id of the frame whose 'evictable' status will be modified
    /// - `set_evictable`: whether the given frame is evictable or not
    pub fn set_evictable(&mut self, frame_id: &FrameId, set_evictable: bool) {
        ////////////////////////////// Begin: Students Implement  //////////////////////////////

        let node = self
            .node_store
            .get_mut(frame_id)
            .expect("No valid LRUk node corresponding to the given frame id {frame_id} exists");

        if node.is_evictable == set_evictable {
            return;
        }
        node.is_evictable = set_evictable;
        match set_evictable {
            true => self.increment_current_size(),
            false => self.decrement_current_size(),
        }

        ////////////////////////////// End: Students Implement  //////////////////////////////
    }

    /// Remove an evictable frame from the replacer, along with its access history.
    /// This function should also decrement replacer's size if removal is successful.
    ///
    /// Note that this is different from evicting a frame, which always removes the frame
    /// with the largest backward k-distance. This function removes the specified frame id,
    /// no matter what its backward k-distance is.
    ///
    /// If `remove` is called on a non-evictable frame, throw an exception or abort the
    /// process.
    ///
    /// If the specified frame is not found, directly return from this function.
    ///
    /// # Parameters
    /// - `frame_id`: id of the frame to be removed
    pub fn remove(&mut self, frame_id: &FrameId) {
        ////////////////////////////// Begin: Students Implement  //////////////////////////////

        let maybe_node = self.node_store.get(frame_id);
        if maybe_node.is_none() {
            return;
        }

        let node = maybe_node.unwrap();
        if !node.is_evictable {
            panic!(
                "Attempted to evict unevictable node with frame id {}",
                frame_id
            );
        }

        self.node_store.remove(frame_id);
        self.decrement_current_size();

        ////////////////////////////// End: Students Implement  //////////////////////////////
    }

    #[allow(dead_code)]
    pub(crate) fn is_full_capacity(&self) -> bool {
        self.curr_size == self.max_size
    }

    // Returns the number of evictable frames in the replacer.
    pub fn size(&self) -> usize {
        self.curr_size
    }

    fn increment_current_size(&mut self) {
        self.curr_size += 1;
    }

    fn decrement_current_size(&mut self) {
        if self.curr_size == 0 {
            panic!("Attempted to decrement current size, which is already 0");
        }
        self.curr_size -= 1;
    }

    ////////////////////////////// Begin: Not Visible to Students //////////////////////////////

    fn evict_frame(&mut self, frame_id: &FrameId) {
        self.node_store.remove(frame_id);
    }

    fn get_frame_to_evict(&self) -> Option<FrameId> {
        let mut evicted_frame_id: Option<&FrameId> = None;
        let mut largest_k_distance = 0_usize;
        // only used for LRU logic in the case of multiple infinite k-distances
        let mut earliest_recent_timestamp = INF;

        self.node_store
            .iter()
            .filter(|(_, node)| node.is_evictable)
            .for_each(|(frame_id, node)| {
                // Select frame to evict based on...
                match largest_k_distance {
                    // ...LRU
                    INF => {
                        if !node.has_infinite_backwards_k_distance() {
                            return;
                        }
                        let timestamp = node.get_most_recent_timestamp();
                        if timestamp > earliest_recent_timestamp {
                            return;
                        }
                        // update evicted frame placeholder
                        earliest_recent_timestamp = timestamp;
                        evicted_frame_id = Some(frame_id);
                    }
                    // ...LRU-k
                    _ => {
                        if node.has_infinite_backwards_k_distance() {
                            earliest_recent_timestamp = node.get_most_recent_timestamp();
                            largest_k_distance = INF;
                            evicted_frame_id = Some(frame_id);
                            return;
                        }
                        let k_distance = node.get_backwards_k_distance(self.current_timestamp);
                        if largest_k_distance > k_distance {
                            return;
                        }
                        // update evicted frame placeholder
                        largest_k_distance = k_distance;
                        evicted_frame_id = Some(frame_id);
                    }
                }
            });
        evicted_frame_id.cloned()
    }

    fn modify_node_history(&mut self, frame_id: &FrameId) {
        if let Some(node) = self.node_store.get_mut(frame_id) {
            // maintains (eyoon's) invariant that node.history.front() is timestamp of k'th access
            if node.history.len() == node.k {
                node.history.pop_front();
            }
            node.history.push_back(self.current_timestamp);
        }
        self.current_timestamp += 1;
    }

    ////////////////////////////// End: Not Visible to Students //////////////////////////////
}

pub struct LRUKReplacerBuilder {
    node_store: HashMap<FrameId, LRUKNode>,
    current_timestamp: usize,
    curr_size: usize,
    max_size: Option<usize>,
    k: Option<usize>,
}

impl LRUKReplacerBuilder {
    pub fn max_size(mut self, num_frames: usize) -> Self {
        assert!(num_frames > 0);
        self.max_size = Some(num_frames);
        self
    }

    pub fn k(mut self, k: usize) -> Self {
        assert!(k > 0);
        self.k = Some(k);
        self
    }

    pub fn build(self) -> LRUKReplacer {
        LRUKReplacer {
            node_store: self.node_store,
            current_timestamp: self.current_timestamp,
            curr_size: self.curr_size,
            max_size: self
                .max_size
                .expect("Replacer size was not specified before build."),
            k: self.k.expect("k was not specified before build."),
        }
    }
}
// eof  ‎‎‎‎
