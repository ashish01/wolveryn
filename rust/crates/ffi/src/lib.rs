//! C FFI for the search engine library.
//!
//! Exposes a minimal C API for use from Go via CGO.

use search_core::searcher::Searcher;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic;
use std::path::Path;
use std::sync::Mutex;

/// Opaque handle to a Searcher instance.
pub struct SearchIndex {
    searcher: Mutex<Searcher>,
}

/// Open an index directory. Returns a pointer to SearchIndex or null on error.
#[no_mangle]
pub extern "C" fn search_index_open(path: *const c_char) -> *mut SearchIndex {
    let result = panic::catch_unwind(|| {
        if path.is_null() {
            return std::ptr::null_mut();
        }
        let c_str = unsafe { CStr::from_ptr(path) };
        let path_str = match c_str.to_str() {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        };
        let searcher = match Searcher::open(Path::new(path_str)) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("search_index_open error: {}", e);
                return std::ptr::null_mut();
            }
        };
        Box::into_raw(Box::new(SearchIndex {
            searcher: Mutex::new(searcher),
        }))
    });
    result.unwrap_or(std::ptr::null_mut())
}

/// Close and free a SearchIndex.
#[no_mangle]
pub extern "C" fn search_index_close(idx: *mut SearchIndex) {
    if !idx.is_null() {
        let _ = panic::catch_unwind(|| {
            unsafe {
                let _ = Box::from_raw(idx);
            }
        });
    }
}

/// Execute a search query. Returns a JSON string (caller must free with search_string_free).
/// Returns null on error.
#[no_mangle]
pub extern "C" fn search_index_search(
    idx: *mut SearchIndex,
    query: *const c_char,
    limit: u32,
    offset: u32,
) -> *mut c_char {
    let result = panic::catch_unwind(|| {
        if idx.is_null() || query.is_null() {
            return std::ptr::null_mut();
        }
        let index = unsafe { &*idx };
        let c_query = unsafe { CStr::from_ptr(query) };
        let query_str = match c_query.to_str() {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        };

        let searcher = index.searcher.lock().unwrap();
        let results = match searcher.search(query_str, limit as usize, offset as usize) {
            Ok(r) => r,
            Err(e) => {
                let err_json = serde_json::json!({"error": e.to_string()});
                let json_str = serde_json::to_string(&err_json).unwrap_or_default();
                return CString::new(json_str).unwrap().into_raw();
            }
        };

        let json_str = serde_json::to_string(&results).unwrap_or_default();
        CString::new(json_str).unwrap().into_raw()
    });
    result.unwrap_or(std::ptr::null_mut())
}

/// Get index statistics. Returns a JSON string (caller must free with search_string_free).
#[no_mangle]
pub extern "C" fn search_index_stats(idx: *mut SearchIndex) -> *mut c_char {
    let result = panic::catch_unwind(|| {
        if idx.is_null() {
            return std::ptr::null_mut();
        }
        let index = unsafe { &*idx };
        let searcher = index.searcher.lock().unwrap();
        let stats = searcher.stats();
        let json_str = serde_json::to_string(&stats).unwrap_or_default();
        CString::new(json_str).unwrap().into_raw()
    });
    result.unwrap_or(std::ptr::null_mut())
}

/// Free a string returned by search_index_search or search_index_stats.
#[no_mangle]
pub extern "C" fn search_string_free(s: *mut c_char) {
    if !s.is_null() {
        let _ = panic::catch_unwind(|| {
            unsafe {
                let _ = CString::from_raw(s);
            }
        });
    }
}
