use crate::common::constants::{NO_CORRESPONDING_FRAME_ID_MSG, NO_CORRESPONDING_PAGE_MSG};
use crate::storage::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
use crate::storage::disk::disk_manager::{DiskManager, PageId};
use crate::storage::page::{Page, TablePage, TablePageHandle};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock, RwLockWriteGuard};

pub type FrameId = usize;

#[derive(Copy, Clone, Debug)]
pub struct FrameMetadata {
    frame_id: FrameId,
    pin_count: usize,
}

impl FrameMetadata {
    pub fn new(frame_id: FrameId) -> Self {
        Self {
            frame_id,
            pin_count: 0,
        }
    }

    #[allow(dead_code)]
    pub fn pin_count(&self) -> usize {
        self.pin_count
    }
    pub fn increment_pin_count(&mut self) {
        self.pin_count += 1;
    }
    pub fn decrement_pin_count(&mut self) {
        if self.pin_count == 0 {
            panic!("Pin count already at zero, cannot decrement.");
        }
        self.pin_count -= 1;
    }

    #[allow(dead_code)]
    pub fn frame_id(&self) -> &FrameId {
        &self.frame_id
    }
}

#[derive(Debug)]
pub struct BufferPoolManager {
    /// Number of page in the buffer pool.
    pub(crate) pool_size: usize,
    /// Array of buffer pool page.
    pub(crate) pages: Vec<TablePageHandle>,
    /// HashMap that maps page IDs to frame IDs (offsets in `page`).
    pub(crate) page_table: HashMap<PageId, FrameMetadata>,
    /// Manages reads and writes of page on disk.
    pub(crate) disk_manager: Arc<RwLock<DiskManager>>,
    /// Replacer to find unpinned page for replacement.
    pub(crate) replacer: Arc<RwLock<LRUKReplacer>>,
    /// List of free frames that don't have any page on them.
    pub(crate) free_list: VecDeque<FrameId>,
}

#[derive(Default)]
pub struct BufferPoolManagerBuilder {
    pool_size: Option<usize>,
    replacer_k: Option<usize>,
    disk_manager: Option<Arc<RwLock<DiskManager>>>,
}

impl BufferPoolManagerBuilder {
    pub fn pool_size(&mut self, pool_size: usize) -> &mut Self {
        self.pool_size = Some(pool_size);
        self
    }
    pub fn replacer_k(&mut self, replacer_k: usize) -> &mut Self {
        self.replacer_k = Some(replacer_k);
        self
    }
    pub fn disk_manager(&mut self, disk_manager: Arc<RwLock<DiskManager>>) -> &mut Self {
        self.disk_manager = Some(disk_manager);
        self
    }
    pub fn build(&self) -> BufferPoolManager {
        let pool_size = self
            .pool_size
            .expect("`pool_size` not initialized before build.");
        let replacer_k = self
            .replacer_k
            .expect("`replacer_k` not initialized before build.");
        let disk_manager = self
            .disk_manager
            .clone()
            .expect("`disk_manager` not initialized before build.");

        BufferPoolManager::new(pool_size, replacer_k, disk_manager)
    }

    pub fn build_with_handle(&self) -> Arc<RwLock<BufferPoolManager>> {
        Arc::new(RwLock::new(self.build()))
    }
}

impl BufferPoolManager {
    pub fn new(
        pool_size: usize,
        replacer_k: usize,
        disk_manager: Arc<RwLock<DiskManager>>,
    ) -> Self {
        BufferPoolManager {
            pool_size,
            pages: Vec::with_capacity(pool_size),
            page_table: HashMap::new(),
            disk_manager,
            replacer: Arc::new(RwLock::new(LRUKReplacer::new(pool_size, replacer_k))),
            free_list: (0..pool_size).collect(),
            // Initialize other fields here
        }
    }

    pub fn new_with_handle(
        pool_size: usize,
        replacer_k: usize,
        disk_manager: Arc<RwLock<DiskManager>>,
    ) -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self::new(pool_size, replacer_k, disk_manager)))
    }

    pub fn builder() -> BufferPoolManagerBuilder {
        BufferPoolManagerBuilder::default()
    }

    /// Creates a new page in the buffer pool.
    ///
    /// This method allocates a new page and returns its identifier. If all
    /// frames are in use and cannot be evicted, it returns `None`.
    ///
    /// The frame should be pinned to prevent eviction, and its access history
    /// recorded.
    ///
    /// # Returns
    /// - `Some(PageId)`: The identifier of the newly created page if successful.
    /// - `None`: If no new page could be created due to all frames being in use.
    pub fn new_page(&mut self) -> Option<PageId> {
        ////////////////////////////// Begin: Students Implement  //////////////////////////////

        let replacer_binding = Arc::clone(&self.replacer);
        let mut replacer = replacer_binding.write().unwrap();

        let frame_id = self.get_free_frame(&mut replacer)?;

        let disk_binding = Arc::clone(&self.disk_manager);
        let mut disk_writer = disk_binding.write().unwrap();

        let page_id = disk_writer.allocate_new_page();

        self.insert_page_from_disk_into_buffer(&page_id, frame_id, &mut disk_writer);
        self.record_access(frame_id, &mut replacer);
        self.increment_pin_count(&page_id);

        Some(page_id)

        ////////////////////////////// End: Students Implement  //////////////////////////////
    }

    /// Fetches a page from the buffer pool.
    ///
    /// This method attempts to retrieve the page identified by `page_id` from
    /// the buffer pool. If the page is not in the pool and all frames are
    /// currently in use and non-evictable (i.e., pinned), it returns `None`.
    ///
    /// The function first searches for the `page_id` in the buffer pool. If
    /// the page is not found, it selects a frame from the free list or, if
    /// empty, from the replacer, reading the page from disk and adding it to
    /// the buffer pool.
    ///
    /// Additionally, eviction is disabled for the frame, and its access history
    /// is recorded similarly to `NewPage`.
    ///
    /// Note: it is undefined behavior to call `fetch_page` on a `page_id` that
    /// does not exist in the page.
    ///
    /// # Parameters
    /// - `page_id`: The identifier of the page to be fetched.
    ///
    /// # Returns
    /// - `Some(&mut TablePage)`: A mutable reference to the page if it is
    ///   successfully fetched.
    /// - `None`: If the `page_id` cannot be fetched due to all frames being
    ///   in use and non-evictable.
    pub fn fetch_page(&mut self, page_id: &PageId) -> Option<TablePageHandle> {
        ////////////////////////////// Begin: Students Implement  //////////////////////////////

        let disk_binding = Arc::clone(&self.disk_manager);
        let mut disk_writer = disk_binding.write().unwrap();
        let replacer_binding = Arc::clone(&self.replacer);
        let mut replacer = replacer_binding.write().unwrap();

        let maybe_frame_id = self.page_table.get(page_id).copied().map_or_else(
            || {
                let frame_id = self.get_free_frame(&mut replacer)?;
                self.insert_page_from_disk_into_buffer(page_id, frame_id, &mut disk_writer);
                Some(frame_id)
            },
            |metadata| Some(metadata.frame_id),
        );

        let frame_id = maybe_frame_id?;
        self.record_access(frame_id, &mut replacer);
        self.increment_pin_count(page_id);

        self.pages.get(frame_id).map(Arc::clone)

        ////////////////////////////// End: Students Implement  //////////////////////////////
    }

    /// Unpins a page from the buffer pool.
    ///
    /// This method attempts to unpin the page identified by `page_id` from the
    /// buffer pool. If the page is not present in the pool, it should abort; or,
    /// if the page's pin count is already zero, the function returns `false` to
    /// indicate that no action was taken.
    ///
    /// When unpinning a page, the method decrements its pin count. If the pin
    /// count drops to zero, the frame containing the page becomes eligible for
    /// eviction by the replacer. The function also updates the page's dirty flag
    /// based on the `is_dirty` parameter, which indicates whether the page has
    /// been modified.
    ///
    /// # Parameters
    /// - `page_id`: The identifier of the page to be unpinned.
    /// - `is_dirty`: A boolean flag that specifies whether the page should be
    ///   marked as dirty (`true`) or clean (`false`).
    ///
    /// # Returns
    /// - `true`: If the page was successfully unpinned (i.e., it was present
    ///   in the buffer pool and its pin count was greater than zero before this
    ///   call).
    /// - `false`: If the page was not in the buffer pool or its pin count was
    ///   zero or less before this call.
    pub fn unpin_page(&mut self, page_id: &PageId, is_dirty: bool) -> bool {
        ////////////////////////////// Begin: Students Implement  //////////////////////////////
        let pin_count = self
            .get_pin_count(page_id)
            .expect(NO_CORRESPONDING_PAGE_MSG);
        match pin_count {
            0 => false,
            1 => {
                let binding = Arc::clone(&self.replacer);
                let mut replacer = binding.write().unwrap();

                self.decrement_pin_count(page_id);
                self.set_is_dirty(page_id, is_dirty);
                self.set_evictable(page_id, true, &mut replacer);
                true
            }
            _ => {
                self.decrement_pin_count(page_id);
                self.set_is_dirty(page_id, is_dirty);
                true
            }
        }
        ////////////////////////////// End: Students Implement  //////////////////////////////
    }

    /// Flushes a page to disk.
    ///
    /// This method writes the page identified by `page_id` to disk using
    /// the [`crate::storage::disk::disk_manager::DiskManager::write_page`] method.
    /// This operation is performed regardless of the page's dirty flag.
    /// After the page is successfully flushed, its dirty flag is reset to
    /// indicate that the page is now clean.
    ///
    /// If the page corresponding to `page_id` does not exist in the page,
    /// this method should abort.
    ///
    /// # Parameters
    /// - `page_id`: The identifier of the page to be flushed.
    pub fn flush_page(&mut self, page_id: &PageId) {
        ////////////////////////////// Begin: Students Implement  //////////////////////////////

        let binding = Arc::clone(&self.disk_manager);
        let mut disk_writer = binding.write().unwrap();

        let page_binding = self.get_page(page_id).expect(NO_CORRESPONDING_PAGE_MSG);
        let mut page = page_binding.write().unwrap();

        disk_writer.write_page(page.clone());
        page.set_is_dirty(false);

        ////////////////////////////// End: Students Implement  //////////////////////////////
    }

    /// Flush all the page in the buffer pool to disk.
    pub fn flush_all_pages(&mut self) {
        ////////////////////////////// Begin: Students Implement  //////////////////////////////

        let page_ids: Vec<PageId> = self.page_table.keys().cloned().collect();

        for page_id in page_ids {
            self.flush_page(&page_id);
        }

        ////////////////////////////// End: Students Implement  //////////////////////////////
    }

    /// If the page identified by `page_id` is not in the buffer pool, this
    /// method aborts. If the page is pinned, it returns `false`. Otherwise,
    /// it deletes the page, updates the frame list,
    /// ([maybe] resets the page's memory and metadata, ) and calls
    /// [`crate::storage::disk::disk_manager::DiskManager::deallocate_page`] to free it
    /// on disk.
    ///
    /// # Parameters
    /// - `page_id`: The identifier of the page to be deleted.
    ///
    /// # Returns
    /// - `true`: If the page was successfully deleted.
    /// - `false`: If the page was found but could not be deleted (e.g., it was pinned).
    pub fn delete_page(&mut self, page_id: PageId) -> bool {
        ////////////////////////////// Begin: Students Implement  //////////////////////////////

        let pin_count = self
            .get_pin_count(&page_id)
            .expect(NO_CORRESPONDING_PAGE_MSG);

        // page is unevictable.
        if pin_count > 0 {
            return false;
        }

        let disk_binding = Arc::clone(&self.disk_manager);
        let mut disk_writer = disk_binding.write().unwrap();
        let replacer_binding = Arc::clone(&self.replacer);
        let mut replacer = replacer_binding.write().unwrap();

        self.remove_from_buffer(&page_id, &mut replacer);
        disk_writer.deallocate_page(&page_id);
        true

        ////////////////////////////// End: Students Implement  //////////////////////////////
    }

    pub fn size(&self) -> usize {
        self.pool_size
    }

    pub(crate) fn get_is_dirty(&self, page_id: &PageId) -> bool {
        let frame_id = self
            .page_table
            .get(page_id)
            .expect(NO_CORRESPONDING_FRAME_ID_MSG)
            .frame_id;
        self.pages.get(frame_id).unwrap().read().unwrap().is_dirty
    }

    pub(crate) fn get_pin_count(&self, page_id: &PageId) -> Option<usize> {
        Some(self.page_table.get(page_id)?.pin_count)
    }

    pub(crate) fn set_is_dirty(&mut self, page_id: &PageId, is_dirty: bool) {
        let frame_id = self
            .page_table
            .get(page_id)
            .expect(NO_CORRESPONDING_FRAME_ID_MSG)
            .frame_id;
        self.pages
            .get_mut(frame_id)
            .unwrap()
            .write()
            .unwrap()
            .set_is_dirty(is_dirty);
    }

    pub(crate) fn set_evictable(
        &mut self,
        page_id: &PageId,
        is_evictable: bool,
        replacer: &mut RwLockWriteGuard<LRUKReplacer>,
    ) {
        let frame_id = self
            .page_table
            .get(page_id)
            .expect(NO_CORRESPONDING_FRAME_ID_MSG)
            .frame_id;
        replacer.set_evictable(&frame_id, is_evictable);
    }

    ////////////////////////////// Begin: Not Visible to Students //////////////////////////////

    /// Called after a page is evicted or removed from the buffer pool, performing necessary update
    /// housekeeping tasks to clean up page, and page vector data structures in the BPM.
    ///
    /// Note: this does NOT add `frame_id` back into free list, as some of its calling contexts will
    /// immediately reoccupy the frame corresponding to `frame_id`. If you wish to add `frame_id` back
    /// to the free list, make sure to do so explicitly.
    pub(crate) fn clean_frame_after_removal(&mut self, frame_id: FrameId, page_id: &PageId) {
        self.page_table.remove(page_id);
        self.pages[frame_id] = Arc::new(RwLock::new(TablePage::create_invalid_page()));
    }

    pub fn evict_from_buffer(
        &mut self,
        replacer: &mut RwLockWriteGuard<LRUKReplacer>,
    ) -> Option<FrameId> {
        let frame_id = replacer.evict()?;

        // clean up evicted page
        let page_id = *self.pages.get(frame_id)?.read().unwrap().page_id();
        if self.get_is_dirty(&page_id) {
            let mut page = self
                .pages
                .get(frame_id)
                .expect("No page at offset {frame_id} exists in page list.")
                .write()
                .unwrap();
            let binding = Arc::clone(&self.disk_manager);
            let mut disk_writer = binding.write().unwrap();
            disk_writer.write_page(page.clone());
            page.set_is_dirty(false);
        }
        // Note: see the note in [`Self::clean_frame_after_removal`]
        // We don't add the frame_id back to the free list since we immediately use it after eviction.
        self.clean_frame_after_removal(frame_id, &page_id);

        Some(frame_id)
    }

    pub fn remove_from_buffer(
        &mut self,
        page_id: &PageId,
        replacer: &mut RwLockWriteGuard<LRUKReplacer>,
    ) {
        let frame_id = self.page_table.get(page_id).unwrap().frame_id;

        replacer.remove(&frame_id);
        self.clean_frame_after_removal(frame_id, page_id);
        // Note: see the note in [`Self::clean_frame_after_removal`]
        // regarding why the evicted frame id is added to the free list here, and not there instead.
        self.free_list.push_back(frame_id);
    }

    pub fn get_free_frame(
        &mut self,
        replacer: &mut RwLockWriteGuard<LRUKReplacer>,
    ) -> Option<FrameId> {
        if let Some(frame_id) = self.free_list.pop_front() {
            return Some(frame_id);
        }
        self.evict_from_buffer(replacer)
    }

    pub fn get_page(&mut self, page_id: &PageId) -> Option<TablePageHandle> {
        self.page_table
            .get(page_id)
            .map(|entry| Arc::clone(&self.pages[entry.frame_id]))
    }

    pub fn increment_pin_count(&mut self, page_id: &PageId) {
        let metadata = self
            .page_table
            .get_mut(page_id)
            .expect(NO_CORRESPONDING_FRAME_ID_MSG);
        metadata.increment_pin_count();
    }

    pub fn decrement_pin_count(&mut self, page_id: &PageId) {
        let metadata = self
            .page_table
            .get_mut(page_id)
            .expect(NO_CORRESPONDING_FRAME_ID_MSG);
        metadata.decrement_pin_count();
    }

    pub fn insert_page_from_disk_into_buffer(
        &mut self,
        page_id: &PageId,
        frame_id: FrameId,
        disk_writer: &mut RwLockWriteGuard<DiskManager>,
    ) {
        // TODO: consider renaming this from disk_writer to disk_reader
        let table_page = Arc::new(RwLock::new(disk_writer.read_page(page_id).clone()));

        // Insert new frame
        self.page_table
            .insert(*page_id, FrameMetadata::new(frame_id));

        // TODO(eyoon): there has to be a better way to do this
        if self.pages.len() <= frame_id {
            self.pages.resize_with(frame_id + 1, || {
                Arc::new(RwLock::new(TablePage::create_invalid_page()))
            });
        }
        self.pages[frame_id] = table_page;
    }

    pub fn record_access(
        &mut self,
        frame_id: FrameId,
        replacer: &mut RwLockWriteGuard<LRUKReplacer>,
    ) {
        replacer.record_access(&frame_id, AccessType::Lookup);
        replacer.set_evictable(&frame_id, false);
    }
    ////////////////////////////// End: Not Visible to Students //////////////////////////////
}

impl Drop for BufferPoolManager {
    fn drop(&mut self) {
        // Code to clean up resources
        println!("BufferPoolManager is being dropped");
    }
}
// eof  ‎‎‎‎
