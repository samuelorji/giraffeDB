pub trait Serialize<T> {
    fn serialize(&self, buffer : &mut [u8]);
}

pub trait Deserialize<T> {
    fn deserialize(&mut self, buffer : &[u8]);
}