use serde::{Deserialize, Serialize};
use std::{mem, ptr, slice};

#[derive(Deserialize)]
struct Input {
    hook: String,
    request: Request,
    #[serde(default)]
    resource: Option<Resource>,
}

#[derive(Deserialize)]
struct Request {
    operation: String,
    #[serde(default)]
    authorization: Authorization,
    #[serde(default)]
    bucket: Option<String>,
    #[serde(default)]
    key: Option<String>,
    #[serde(rename = "isReadOnly")]
    is_read_only: bool,
}

#[derive(Default, Deserialize)]
struct Authorization {
    #[serde(default, rename = "accessKeyId")]
    access_key_id: Option<String>,
}

#[derive(Deserialize)]
struct Resource {
    #[serde(default)]
    key: Option<String>,
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

#[no_mangle]
pub unsafe extern "C" fn pithos_evaluate(ptr: *const u8, len: usize) -> u64 {
    let input = slice::from_raw_parts(ptr, len);
    let Ok(parsed) = serde_json::from_slice::<Input>(input) else {
        return write_decision(false);
    };

    write_decision(allow(&parsed))
}

fn allow(input: &Input) -> bool {
    match input.hook.as_str() {
        "request" => allow_request(&input.request),
        "list-object" => input
            .resource
            .as_ref()
            .and_then(|resource| resource.key.as_deref())
            .map(|key| key.starts_with("public/"))
            .unwrap_or(false),
        _ => false,
    }
}

fn allow_request(request: &Request) -> bool {
    let public_read = request.operation == "GetObject"
        && request.bucket.as_deref() == Some("public-assets")
        && request.key.as_deref() == Some("public/index.html");
    let admin_write = request.authorization.access_key_id.as_deref() == Some("admin-access-key-id")
        && !request.is_read_only;

    public_read || admin_write
}

unsafe fn write_decision(allow: bool) -> u64 {
    let output =
        serde_json::to_vec(&Decision { allow }).unwrap_or_else(|_| b"{\"allow\":false}".to_vec());
    let out_len = output.len();
    let out_ptr = pithos_alloc(out_len);
    ptr::copy_nonoverlapping(output.as_ptr(), out_ptr, out_len);
    ((out_ptr as u64) << 32) | out_len as u64
}
