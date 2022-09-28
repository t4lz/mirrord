use exec::execvp;
use fork::{fork, Fork};
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
    // Fork so that we get to keep a test process, and start a new process to inject the layer into.
    // TODO: make sure child is killed if even if the exits prematurely.
    match fork() {
        Ok(Fork::Parent(child)) => {
            debug!("Continuing test execution in parent process, new child has pid: {}", child);
        }
        Ok(Fork::Child) => {
            trace!("Child process forked from test process.");
            // TODO: Give absolute path.
            add_to_preload("../../target/debug/libmirrord_layer.dylib").unwrap();
            std::env::set_var("MIRRORD_AGENT_IMPERSONATED_POD_NAME", "py-serv-deployment-ff89b5974-42kkh");
            let mut app_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            app_path.push("tests/app.js");
            let err = execvp(String::from("node"), [app_path.to_string_lossy().to_string()]);
            error!("{err}");
            panic!("Failed to execute binary");
        },
        Err(_) => {
            panic!("Fork failed - cannot continue test (the problem is in the execution of the test - not in the tested property).")
        },
    }
}