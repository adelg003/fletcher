use fs_extra::dir::{CopyOptions, copy};
use std::{fs, path::Path, process::Command};

fn main() {
    // Cleanup assets folder
    let build_location = Path::new("assets/");
    if build_location.exists() {
        fs::remove_dir_all(build_location).unwrap();
    }

    // Install Node Dependencies
    let output = Command::new("sh")
        .arg("-c")
        .arg("npm clean-install")
        .output()
        .unwrap();

    // Ensure Node Install worked
    if !output.status.success() {
        panic!(
            "Shell command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Create Assets directory
    fs::create_dir_all("assets/images/").unwrap();

    // Copy all images to assets working folder
    copy("images/", "assets/", &CopyOptions::default()).unwrap();

    // Populate Assets directory with HTMX
    fs::create_dir_all("assets/htmx").unwrap();
    fs::copy(
        "node_modules/htmx.org/dist/htmx.min.js",
        "assets/htmx/htmx.min.js",
    )
    .unwrap();

    // Populate Assets directory with Viz-JS
    fs::create_dir_all("assets/viz").unwrap();
    fs::copy(
        "node_modules/@viz-js/viz/lib/viz-standalone.js",
        "assets/viz/viz-standalone.js",
    )
    .unwrap();

    // Populate Assets directory with Prism.js
    fs::create_dir_all("assets/prism").unwrap();
    fs::copy("node_modules/prismjs/prism.js", "assets/prism/prism.js").unwrap();
    fs::copy(
        "node_modules/prismjs/components/prism-json.js",
        "assets/prism/prism-json.js",
    )
    .unwrap();
    fs::copy(
        "node_modules/prism-themes/themes/prism-holi-theme.css",
        "assets/prism/prism.css",
    )
    .unwrap();

    // Generate TailwindCSS file fs::create_dir_all("assets/tailwindcss").unwrap();
    let output = Command::new("sh")
        .arg("-c")
        .arg("npx @tailwindcss/cli -i ./tailwind.css -o ./assets/tailwindcss/tailwind.css")
        .output()
        .unwrap();

    // Ensure TailwindCSS worked
    if !output.status.success() {
        panic!(
            "Shell command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
