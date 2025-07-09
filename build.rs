use fs_extra::dir::{CopyOptions, copy};
use std::{fs, path::Path};

fn main() {
    // Cleanup tmp folder
    let build_location = Path::new("tmp/");
    if build_location.exists() {
        fs::remove_dir_all(build_location).unwrap();
    }

    // Create Assets directory
    fs::create_dir_all("tmp/assets/images/").unwrap();

    // Copy all images to assets working folder
    copy("images/", "tmp/assets/", &CopyOptions::default()).unwrap();
}
