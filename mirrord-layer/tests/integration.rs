use exec::execvp;
use std::env::temp_dir;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use anyhow::{anyhow, Context, Result};
use rand::distributions::{Alphanumeric, DistString};
use tracing_test::traced_test;
use tracing::{debug, error, info, trace, warn};

#[cfg(target_os = "linux")]
const INJECTION_ENV_VAR: &str = "LD_PRELOAD";

#[cfg(target_os = "macos")]
const INJECTION_ENV_VAR: &str = "DYLD_INSERT_LIBRARIES";

// TODO: can this be removed/shortened?
fn extract_library(dest_dir: Option<String>) -> Result<PathBuf> {
    // let library_file = env!("MIRRORD_LAYER_FILE");
    let library_file = "../../target/debug/libmirrord_layer.dylib"; // TODO!
    let library_path = Path::new(library_file);
    debug!("Using layer lib path {:#}", library_path.to_str().unwrap());

    let extension = library_path
        .components()
        .last()
        .unwrap()
        .as_os_str()
        .to_str()
        .unwrap()
        .split('.')
        .collect::<Vec<&str>>()[1];

    let file_name = format!(
        "{}-libmirrord_layer.{extension}",
        Alphanumeric
            .sample_string(&mut rand::thread_rng(), 10)
            .to_lowercase()
    );
    debug!("Using layer lib file {:#}", file_name);

    let file_path = match dest_dir {
        Some(dest_dir) => std::path::Path::new(&dest_dir).join(file_name),
        None => temp_dir().as_path().join(file_name),
    };
    let mut file = File::create(&file_path)
        .with_context(|| format!("Path \"{}\" creation failed", file_path.display()))?;
    // let bytes = include_bytes!(env!("MIRRORD_LAYER_FILE"));
    let bytes = include_bytes!("../../target/debug/libmirrord_layer.dylib"); // TODO!
    file.write_all(bytes).unwrap();

    debug!("Extracted library file to {:?}", &file_path);
    Ok(file_path)
}

fn add_to_preload(path: &str) -> Result<()> {
    match std::env::var(INJECTION_ENV_VAR) {
        Ok(value) => {
            let new_value = format!("{}:{}", value, path);
            trace!("Injection env var {new_value}.");
            std::env::set_var(INJECTION_ENV_VAR, new_value);
            Ok(())
        }
        Err(std::env::VarError::NotPresent) => {
            trace!("Injection env var {path}.");
            std::env::set_var(INJECTION_ENV_VAR, path);
            Ok(())
        }
        Err(e) => {
            error!("Failed to set environment variable with error {:?}", e);
            Err(anyhow!("Failed to set environment variable"))
        }
    }
}

#[traced_test]
#[test]
fn run_layer() {
    let library_path = extract_library(None).unwrap(); // TODO: remove option arg.
    add_to_preload(library_path.to_str().unwrap()).unwrap();
    std::env::set_var("MIRRORD_AGENT_IMPERSONATED_POD_NAME", "py-serv-deployment-ff89b5974-42kkh");

    let mut app_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    app_path.push("tests/app.js");
    debug!("running app");
    debug!("{}:{}", INJECTION_ENV_VAR, std::env::var(INJECTION_ENV_VAR).unwrap());
    let err = execvp(String::from("node"), [app_path.to_string_lossy().to_string()]);
    // let err = execvp(String::from("bash"), [String::from("-c"), String::from("\"echo; while [[ 1 ]]; do sleep 1; done\"")]);
    error!("{err}");
    panic!("Failed to execute binary");
}