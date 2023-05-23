use std::fs::File;
use std::os::unix::fs::{FileExt, OpenOptionsExt};

pub const PageSize : u32 = 4096;
pub struct Dal {
    file : File,
    pageSize : u32
}

impl Dal{
    pub fn new(path : &str, pageSize : u32) -> Self {
        let file =  std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .mode(0o666)
            .open(path)
            .unwrap();

        Dal {
            file,
            pageSize
        }
    }

    pub fn allocateEmptyPage(&self, pageNumber : Option<u64>) -> Page {
        Page::new(pageNumber)
    }

    fn readPage(&self, pageNumber : u64) -> Page {
        let mut page = self.allocateEmptyPage(Some(pageNumber));
        let offset = pageNumber * self.pageSize as u64;
        self.file.read_at(&mut page.data, offset).unwrap();
        page
    }

    pub fn writePage(&mut self, page : &mut Page) {
        let offset = page.pageNumber * self.pageSize as u64;
        self.file.write_at(&page.data, offset).unwrap();
    }

}

pub struct Page {
    pub pageNumber : u64,
    pub data : Vec<u8>
}

impl  Page {
    pub fn new(pageNumber : Option<u64>) -> Self {
        Page {
            pageNumber: pageNumber.unwrap_or(0), // if no page number, set to 0
            data: vec![0_u8;  PageSize as usize] // create a 4kB zero filled vector
        }
    }
}