use fs_extra::dir::{CopyOptions, copy};
use std::{fs, path::Path, process::Command};

fn main() {
    // Cleanup assets folder
    let build_location = Path::new("assets/");
    if build_location.exists() {
        fs::remove_dir_all(build_location).expect("Failed to remove existing assets directory");
    }

    // Create Assets directory
    fs::create_dir_all("assets/").expect("Failed to create assets directory");

    // Install Node Dependencies
    let output = Command::new("sh")
        .arg("-c")
        .arg("npm clean-install")
        .output()
        .expect("Failed to execute \"npm clean-install\" command");

    // Ensure Node Install worked
    if !output.status.success() {
        panic!(
            "npm clean-install failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Copy all images to assets working folder
    fs::create_dir_all("assets/images/").expect("Failed to create assets/images directory");
    copy("images/", "assets/", &CopyOptions::default())
        .expect("Failed to copy images directory to assets");

    // Populate Assets directory with HTMX
    fs::create_dir_all("assets/htmx").expect("Failed to create assets/htmx directory");
    fs::copy(
        "node_modules/htmx.org/dist/htmx.min.js",
        "assets/htmx/htmx.min.js",
    )
    .expect("Failed to copy htmx.min.js from node_modules");

    // Populate Assets directory with Viz-JS
    fs::create_dir_all("assets/viz").expect("Failed to create assets/viz directory");
    fs::copy(
        "node_modules/@viz-js/viz/lib/viz-standalone.js",
        "assets/viz/viz-standalone.js",
    )
    .expect("Failed to copy viz-standalone.js from node_modules");

    // Populate Assets directory with Prism.js
    fs::create_dir_all("assets/prism").expect("Failed to create assets/prism directory");
    fs::copy("node_modules/prismjs/prism.js", "assets/prism/prism.js")
        .expect("Failed to copy prism.js from node_modules");
    fs::copy(
        "node_modules/prismjs/components/prism-json.js",
        "assets/prism/prism-json.js",
    )
    .expect("Failed to copy prism-json.js from node_modules");
    fs::copy(
        "node_modules/prism-themes/themes/prism-holi-theme.css",
        "assets/prism/prism.css",
    )
    .expect("Failed to copy prism-holi-theme.css from node_modules");

    // Generate TailwindCSS file
    fs::create_dir_all("assets/tailwindcss")
        .expect("Failed to create assets/tailwindcss directory");
    let output = Command::new("sh")
        .arg("-c")
        .arg("npx @tailwindcss/cli -i ./tailwind.css -o ./assets/tailwindcss/tailwind.css")
        .output()
        .expect("Failed to execute TailwindCSS build command");

    // Ensure TailwindCSS worked
    if !output.status.success() {
        panic!(
            "TailwindCSS build failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
