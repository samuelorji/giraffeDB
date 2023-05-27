use std::fs::File;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;
use crate::util::{Deserialize, Serialize};
use byteorder::{ByteOrder, LittleEndian};
pub const PageSize : u32 = 4096;
pub const META_PAGE_NUMBER: u16 = 0;
pub const PAGE_SIZE: u16 = 4096;

pub struct Dal {
    file : File,
    pageSize : u16,
    freeList: FreeList,
    meta: Meta
}

impl Dal{
    pub fn new(path: &str) -> Self {
        if (!Path::new(path).exists()) {
            // database file doesn't exist, create a new database file
            let file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .append(true)
                .mode(0o666)
                .open(path)
                .unwrap();

            let mut freeList = FreeList::new();
            let mut meta = Meta::new();

            let mut dal = Dal {
                file,
                pageSize: PAGE_SIZE, // PAGE_SIZE is 4KB (4096)
                meta,
                freeList,
            };

            // set freelist to page 1 and increment maxPage in the freelist struct
            let freeListPageNumber = dal.freeList.getNextPageNumber();

            dal.meta.freeListPage = freeListPageNumber;
            dal
        } else {
            // file exists, read meta to find freelist page and deserialize accordingly
            let file = std::fs::OpenOptions::new()
                .write(true)
                .read(true)
                .append(true)
                .open(path)
                .unwrap();

            let mut freeList = FreeList::new();
            let mut meta = Meta::new();
            let mut dal = Dal {
                file,
                pageSize: PAGE_SIZE,
                meta,
                freeList,
            };

            // read meta from disk and store in dal struct
            dal.readMeta();

            // read freeList from and store in dal struct
            dal.readFreeList();
            dal
        }
    }

    pub fn allocateEmptyPage(&self, pageNumber : u16) -> Page {
        Page::new(pageNumber)
    }

    fn readPage(&self, pageNumber : u16) -> Page {
        let mut page = self.allocateEmptyPage(pageNumber);
        let offset = pageNumber as u64 * self.pageSize as u64;
        self.file.read_at(&mut page.data, offset).unwrap();
        page
    }

    pub fn writePage(&mut self, page : &mut Page) {
        let offset = page.pageNumber as u64 * self.pageSize as u64;
        self.file.write_at(&page.data, offset).unwrap();
    }

    pub fn writeMeta(&mut self) -> Page {
        // meta page is 0
        let mut page =  self.allocateEmptyPage(META_PAGE_NUMBER); // META_PAGE_NUMBER = 0

        // serialize the meta struct into the `page.data` buffer
        self.meta.serialize(&mut page.data[..]);

        // write the page to disk
        self.writePage(&mut page);

        page
    }

    pub fn readMeta(&mut self) {
        // read page
        let page = self.readPage(META_PAGE_NUMBER); // META_PAGE_NUMBER = 0

        // create an empty meta
        let mut meta = Meta::new();

        // deserialize what's in the read `page.data` into the empty meta struct
        meta.deserialize(&page.data);
        self.meta = meta;
    }

    pub fn writeFreeList(&mut self) -> Page {
        // get freelist page from meta
        let freeListPage = self.meta.freeListPage;

        let mut page = self.allocateEmptyPage(freeListPage);

        // serialize free list into the empty page
        self.freeList.serialize(&mut page.data);

        // write page
        self.writePage(&mut page);
        page
    }

    pub fn readFreeList(&mut self) {
        // get freelist page from meta
        let freeListPage = self.meta.freeListPage;

        // read freelist page
        let mut page = self.readPage(freeListPage);

        // deserialize free list page
        self.freeList.deserialize(&mut page.data);

    }

    pub fn getNextPageNumber(&mut self) -> u16 {
        self.freeList.getNextPageNumber()

    }

    pub fn releasePage(&mut self, pageNumber : u16) {
        self.freeList.releasePage(pageNumber);
    }
}

impl Drop for Dal {
    fn drop(&mut self) {
        self.writeMeta();
        self.writeFreeList();
    }
}

pub struct Page {
    pub pageNumber : u16,
    pub data : Vec<u8>
}

impl  Page {
    pub fn new(pageNumber : u16) -> Self {
        Page {
            pageNumber, // if no page number, set to 0
            data: vec![0_u8;  PageSize as usize] // create a 4kB zero filled vector
        }
    }
}


#[derive(Debug)]
struct FreeList {
    maxPage : u16, // Holds the maximum page allocated. maxPage * PageSize = databaseFileSize
    releasedPages: Vec<u16> // Pages that were previously allocated but are now free
}

impl FreeList {
    pub fn new() -> Self {
        Self {
            maxPage : 0,
            releasedPages: vec![]
        }
    }

    pub fn getNextPageNumber(&mut self) -> u16 {
        // if possible, get pages from released pages first, else increment maxPage and return it
        if let Some(releasedPageId) = self.releasedPages.pop() {
            releasedPageId
        } else {
            self.maxPage += 1 ;
            self.maxPage
        }
    }

    pub fn releasePage(&mut self, pageNumber : u16) {
        self.releasedPages.push(pageNumber)
    }
}

#[derive(Debug)]
struct Meta {
    freeListPage : u16
}

impl Meta {
    pub  fn new() -> Self {
        Self {
            // start with a seed value of 0, this may change later on
            freeListPage : 0
        }
    }
}

impl Serialize<Meta> for Meta {
    fn serialize(&self, buffer: &mut [u8]) {
        // To serialize a Meta
        // - 8 bytes for the freelist page
        let mut cursor: usize = 0;
        LittleEndian::write_u16(&mut buffer[cursor..], self.freeListPage);
        cursor += 8;
    }
}

impl Deserialize<Meta> for Meta {
    fn deserialize(&mut self, buffer: &[u8]) {
        let mut cursor: usize = 0;
        // read 8 bytes for freelist cursor at cursor `cursor`
        let freeListPage = LittleEndian::read_u16(&buffer[cursor..]);
        cursor += 2;

        self.freeListPage = freeListPage
    }
}

impl Serialize<FreeList> for FreeList {
    fn serialize(&self, buffer: &mut [u8]) {

        // To serialize a Meta
        // - 2 bytes for max page (u16)
        // - 2 bytes for length of released list (vector in our case)
        // - 2 bytes for each released page number

        // write max page (2 bytes)
        let mut cursor: usize = 0;
        LittleEndian::write_u16(&mut buffer[cursor ..], self.maxPage);
        cursor += 2;

        // write length of released pages (2 bytes)
        LittleEndian::write_u16(&mut buffer[cursor ..], self.releasedPages.len() as u16);
        cursor += 2;

        // for each released page, write the released page number (2 bytes)
        for pgNumber in &self.releasedPages {
            LittleEndian::write_u16(&mut buffer[cursor ..], *pgNumber);
            cursor += 2;
        }
    }
}

impl Deserialize<FreeList> for FreeList {
    fn deserialize(&mut self, buffer: &[u8]) {

        // To deserialize a Meta
        // - 2 bytes for max page (u16)
        // - 2 bytes for length of released list (vector in our case)
        // - 2 bytes for each released page number

        // read max page (2 bytes)
        let mut cursor : usize = 0;
        let maxPage = LittleEndian::read_u16(&buffer);
        cursor +=2;

        // read length of released pages (2 bytes)
        let numReleasedPages = LittleEndian::read_u16(&buffer[cursor .. ]);
        cursor +=2;

        let mut releasedPages : Vec<u16> = Vec::with_capacity(numReleasedPages as usize);

        // for each released page, read the released page number (2 bytes)
        for _ in 0..numReleasedPages {
            let releasedPage = LittleEndian::read_u16(&buffer[cursor as usize ..]);
            releasedPages.push(releasedPage);
            cursor += 2

        }

        self.maxPage = maxPage ;
        self.releasedPages = releasedPages
    }
}