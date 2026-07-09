use std::slice;

mod pithos;

use pithos::{Input, Request};

#[no_mangle]
pub unsafe extern "C" fn pithos_evaluate(ptr: *const u8, len: usize) -> u64 {
    let input = slice::from_raw_parts(ptr, len);
    let Ok(parsed) = serde_json::from_slice::<Input>(input) else {
        return pithos::write_decision(false);
    };

    pithos::write_decision(allow(&parsed))
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
