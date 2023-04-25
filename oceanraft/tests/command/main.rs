#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]
#[macro_use]
#[path = "../fixtures/mod.rs"]
mod fixtures;

mod t10_bad_write;
mod t20_basic_write;
mod t30_stale_write;
mod t40_read_index;
mod t50_storage_failure;