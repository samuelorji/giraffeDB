use crate::dal::Dal;

mod dal;

fn main() {
    // initialize Dal
    let mut dal = Dal::new("giraffe.db", dal::PageSize);

    let mut page_0 = dal.allocateEmptyPage(Some(0));

    writeToPage(&mut page_0.data, "Page 0, hello", 0);

    dal.writePage(&mut page_0);

    let mut page_1 = dal.allocateEmptyPage(Some(1));

    writeToPage(&mut page_1.data, "Page 1, hello", 0);

    // commit page
    dal.writePage(&mut page_1);

}

fn writeToPage(buffer: &mut[u8],msg : &str, startAt: usize) {
    // write to buffer from position `startAt`
    buffer[startAt .. msg.len()].copy_from_slice(msg.as_bytes());
}
