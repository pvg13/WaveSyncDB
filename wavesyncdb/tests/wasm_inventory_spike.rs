//! Spike: prove `inventory` collection+iteration works on wasm32 under
//! wasm-bindgen (ctors run at module instantiation). Gates the #93
//! scope-registration design; superseded by real tests in later tasks.
#![cfg(target_arch = "wasm32")]

use wasm_bindgen_test::*;
wasm_bindgen_test_configure!(run_in_browser);

pub struct SpikeRecord(pub &'static str);
inventory::collect!(SpikeRecord);
inventory::submit! { SpikeRecord("alpha") }
inventory::submit! { SpikeRecord("beta") }

#[wasm_bindgen_test]
fn inventory_iterates_on_wasm() {
    let mut names: Vec<&str> = inventory::iter::<SpikeRecord>().map(|r| r.0).collect();
    names.sort_unstable();
    assert_eq!(names, vec!["alpha", "beta"]);
}
