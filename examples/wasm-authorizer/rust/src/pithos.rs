use serde::{Deserialize, Serialize};
use std::{mem, ptr};

#[derive(Deserialize)]
pub struct Input {
    pub hook: String,
    pub request: Request,
    #[serde(default)]
    pub resource: Option<Resource>,
}

#[derive(Deserialize)]
pub struct Request {
    pub operation: String,
    #[serde(default)]
    pub authorization: Authorization,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(rename = "isReadOnly")]
    pub is_read_only: bool,
}

#[derive(Default, Deserialize)]
pub struct Authorization {
    #[serde(default, rename = "accessKeyId")]
    pub access_key_id: Option<String>,
}

#[derive(Deserialize)]
pub struct Resource {
    #[serde(default)]
    pub key: Option<String>,
}

#[derive(Serialize)]
struct Decision {
    allow: bool,
}

#[no_mangle]
pub extern "C" fn pithos_alloc(size: usize) -> *mut u8 {
    let mut buf = Vec::<u8>::with_capacity(size);
    let ptr = buf.as_mut_ptr();
    mem::forget(buf);
    ptr
}

#[no_mangle]
pub unsafe extern "C" fn pithos_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        drop(Vec::from_raw_parts(ptr, 0, len));
    }
}

pub unsafe fn write_decision(allow: bool) -> u64 {
    let output =
        serde_json::to_vec(&Decision { allow }).unwrap_or_else(|_| b"{\"allow\":false}".to_vec());
    let out_len = output.len();
    let out_ptr = pithos_alloc(out_len);
    ptr::copy_nonoverlapping(output.as_ptr(), out_ptr, out_len);
    ((out_ptr as u64) << 32) | out_len as u64
}
