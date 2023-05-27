use std::collections::VecDeque;
use crate::dal::Dal;

mod dal;
mod util;

fn main() {

    // initialize Dal
    let mut dal = Dal::new("giraffe.db");

    // create , write and commit page
    writeAndCommitPage(&mut dal, "first page");
    drop(dal);


    // re initialize Dal
    let mut dal = Dal::new("giraffe.db");
    // create , write and commit page
    writeAndCommitPage(&mut dal, "second page");
    drop(dal);


    // re initialize Dal
    let mut dal = Dal::new("giraffe.db");
    let page_4 = dal.getNextPageNumber();
    dal.allocateEmptyPage(page_4);
    // release the page to test the free list
    dal.releasePage(page_4);
}

fn writeToPage(buffer: &mut[u8],msg : &str, startAt: usize) {
    // write to buffer from position `startAt`
    buffer[startAt .. msg.len()].copy_from_slice(msg.as_bytes());
}

fn writeAndCommitPage(dal : &mut Dal, data : &str )  {

    //creates a page and writes the string to that page
    let nextPageNumber = dal.getNextPageNumber();

    let mut page = dal.allocateEmptyPage(nextPageNumber);

    // write some data to the created page
    writeToPage(&mut page.data, data, 0);

    //persist data to disk
    dal.writePage(&mut page);

}
