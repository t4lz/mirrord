#![feature(stmt_expr_attributes)]
#[cfg(test)]

mod tests {
    use std::{
        cmp::max,
        collections::HashMap,
        fmt::Debug,
        net::{Ipv4Addr, UdpSocket},
        process::Stdio,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use bytes::Bytes;
    use chrono::Utc;
    use futures::{Future, Stream};
    use futures_util::stream::{StreamExt, TryStreamExt};
    use k8s_openapi::api::{
        apps::v1::Deployment,
        core::v1::{Pod, Service},
    };
    use kube::{
        api::{DeleteParams, ListParams, LogParams, PostParams},
        core::WatchEvent,
        runtime::wait::{await_condition, conditions::is_pod_running},
        Api, Client, Config,
    };
    use rand::{distributions::Alphanumeric, Rng};
    use reqwest::StatusCode;
    use rstest::*;
    use serde::{de::DeserializeOwned, Serialize};
    use serde_json::json;
    use tempdir::TempDir;
    use tokio::{
        io::{AsyncReadExt, BufReader},
        process::{Child, Command},
        task::JoinHandle,
        time::timeout,
    };
    // 0.8
    use tokio_util::sync::{CancellationToken, DropGuard};

    static TEXT: &str = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    const CONTAINER_NAME: &str = "test";

    pub async fn watch_resource_exists<K: Debug + Clone + DeserializeOwned>(
        api: &Api<K>,
        name: &str,
    ) {
        let params = ListParams::default()
            .fields(&format!("metadata.name={}", name))
            .timeout(10);
        let mut stream = api.watch(&params, "0").await.unwrap().boxed();
        while let Some(status) = stream.try_next().await.unwrap() {
            match status {
                WatchEvent::Modified(_) => break,
                WatchEvent::Error(s) => {
                    panic!("Error watching namespaces: {:?}", s);
                }
                _ => {}
            }
        }
    }

    fn random_string() -> String {
        let mut rand_str: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();
        rand_str.make_ascii_lowercase();
        rand_str
    }

    #[derive(Debug)]
    enum Application {
        PythonFlaskHTTP,
        PythonFastApiHTTP,
        NodeHTTP,
        Go18HTTP,
        Go19HTTP,
    }

    #[derive(Debug)]
    pub enum Agent {
        #[cfg(target_os = "linux")]
        Ephemeral,
        Job,
    }

    #[derive(Debug)]
    pub enum FileOps {
        Python,
        Go18,
        Go19,
        Rust,
    }

    struct TestProcess {
        pub child: Child,
        stderr: Arc<Mutex<String>>,
        stdout: Arc<Mutex<String>>,
        // Keeps tempdir existing while process is running.
        _tempdir: TempDir,
    }

    impl TestProcess {
        fn get_stdout(&self) -> String {
            self.stdout.lock().unwrap().clone()
        }

        fn assert_stderr(&self) {
            assert!(self.stderr.lock().unwrap().is_empty());
        }

        fn assert_log_level(&self, stderr: bool, level: &str) {
            if stderr {
                assert!(!self.stderr.lock().unwrap().contains(level));
            } else {
                assert!(!self.stdout.lock().unwrap().contains(level));
            }
        }

        fn assert_python_fileops_stderr(&self) {
            assert!(!self.stderr.lock().unwrap().contains("FAILED"));
        }

        /// Check stdout for string, assuming it was already searched until the `covered` position.
        /// Update `covered` to new stdout length.
        fn search_str_in_stdout(&self, covered: &mut usize, searched_for: &str) -> bool {
            let stdout_mutex = self.stdout.lock().unwrap();
            let stdout = stdout_mutex.as_str();
            let len = stdout.len();
            if &len == covered {
                return false; // No new bytes, if string were there, should have been found already.
            }
            let old_covered = *covered;
            *covered = len;

            // Don't include covered positions in search.
            // Start one position after the string's length before the end.
            // All earlier positions were already covered.
            stdout[max(old_covered - searched_for.len() + 1, 0)..].contains(searched_for)
        }

        fn wait_for_line(&self, timeout: Duration, line: &str) {
            let now = std::time::Instant::now();
            let mut covered = 0;
            while now.elapsed() < timeout {
                if self.search_str_in_stdout(&mut covered, line) {
                    return;
                }
            }
            panic!("Timeout waiting for line: {}", line);
        }

        fn from_child(mut child: Child, tempdir: TempDir) -> TestProcess {
            let stderr_data = Arc::new(Mutex::new(String::new()));
            let stdout_data = Arc::new(Mutex::new(String::new()));
            let child_stderr = child.stderr.take().unwrap();
            let child_stdout = child.stdout.take().unwrap();
            let stderr_data_reader = stderr_data.clone();
            let stdout_data_reader = stdout_data.clone();
            let pid = child.id().unwrap();

            tokio::spawn(async move {
                let mut reader = BufReader::new(child_stderr);
                let mut buf = [0; 1024];
                loop {
                    let n = reader.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }

                    let string = String::from_utf8_lossy(&buf[..n]);
                    eprintln!("stderr {:?} {pid}: {}", Utc::now(), string);
                    {
                        stderr_data_reader.lock().unwrap().push_str(&string);
                    }
                }
            });
            tokio::spawn(async move {
                let mut reader = BufReader::new(child_stdout);
                let mut buf = [0; 1024];
                loop {
                    let n = reader.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    let string = String::from_utf8_lossy(&buf[..n]);
                    println!("stdout {:?} {pid}: {}", Utc::now(), string);
                    {
                        stdout_data_reader.lock().unwrap().push_str(&string);
                    }
                }
            });
            TestProcess {
                child,
                stderr: stderr_data,
                stdout: stdout_data,
                _tempdir: tempdir,
            }
        }
    }

    impl Application {
        async fn run(
            &self,
            target: &str,
            namespace: Option<&str>,
            args: Option<Vec<&str>>,
            env: Option<Vec<(&str, &str)>>,
        ) -> TestProcess {
            let process_cmd = match self {
                Application::PythonFlaskHTTP => {
                    vec!["python3", "-u", "python-e2e/app_flask.py"]
                }
                Application::PythonFastApiHTTP => {
                    vec![
                        "uvicorn",
                        "--port=80",
                        "--host=0.0.0.0",
                        "--app-dir=./python-e2e/",
                        "app_fastapi:app",
                    ]
                }
                Application::NodeHTTP => vec!["node", "node-e2e/app.js"],
                Application::Go18HTTP => vec!["go-e2e/18"],
                Application::Go19HTTP => vec!["go-e2e/19"],
            };
            run(process_cmd, target, namespace, args, env).await
        }

        fn assert(&self, process: &TestProcess) {
            match self {
                Application::PythonFastApiHTTP => {
                    process.assert_log_level(true, "ERROR");
                    process.assert_log_level(false, "ERROR");
                    process.assert_log_level(true, "CRITICAL");
                    process.assert_log_level(false, "CRITICAL");
                }
                _ => process.assert_stderr(),
            }
        }
    }

    impl Agent {
        fn flag(&self) -> Option<Vec<&str>> {
            match self {
                #[cfg(target_os = "linux")]
                Agent::Ephemeral => Some(vec!["--ephemeral-container"]),
                Agent::Job => None,
            }
        }
    }

    impl FileOps {
        fn command(&self) -> Vec<&str> {
            match self {
                FileOps::Python => {
                    vec!["python3", "-B", "-m", "unittest", "-f", "python-e2e/ops.py"]
                }
                FileOps::Go18 => vec!["go-e2e-fileops/18"],
                FileOps::Go19 => vec!["go-e2e-fileops/19"],
                FileOps::Rust => vec!["../target/debug/rust-e2e-fileops"],
            }
        }

        fn assert(&self, process: TestProcess) {
            match self {
                FileOps::Python => process.assert_python_fileops_stderr(),
                _ => process.assert_stderr(),
            }
        }
    }

    async fn run(
        process_cmd: Vec<&str>,
        target: &str,
        namespace: Option<&str>,
        args: Option<Vec<&str>>,
        env: Option<Vec<(&str, &str)>>,
    ) -> TestProcess {
        let path = match option_env!("MIRRORD_TESTS_USE_BINARY") {
            None => env!("CARGO_BIN_FILE_MIRRORD"),
            Some(binary_path) => binary_path,
        };
        let temp_dir = tempdir::TempDir::new("test").unwrap();
        let mut mirrord_args = vec![
            "exec",
            "--target",
            target,
            "-c",
            "--extract-path",
            temp_dir.path().to_str().unwrap(),
        ];
        if let Some(namespace) = namespace {
            mirrord_args.extend(["--target-namespace", namespace].into_iter());
        }
        if let Some(args) = args {
            mirrord_args.extend(args.into_iter());
        }
        mirrord_args.push("--");
        let args: Vec<&str> = mirrord_args
            .into_iter()
            .chain(process_cmd.into_iter())
            .collect();
        // used by the CI, to load the image locally:
        // docker build -t test . -f mirrord/agent/Dockerfile
        // minikube load image test:latest
        let mut base_env = HashMap::new();
        base_env.insert("MIRRORD_AGENT_IMAGE", "test");
        base_env.insert("MIRRORD_CHECK_VERSION", "false");
        base_env.insert("MIRRORD_AGENT_RUST_LOG", "warn,mirrord=trace");
        base_env.insert("MIRRORD_AGENT_COMMUNICATION_TIMEOUT", "180");
        base_env.insert("RUST_LOG", "warn,mirrord=trace");

        if let Some(env) = env {
            for (key, value) in env {
                base_env.insert(key, value);
            }
        }

        let server = Command::new(path)
            .args(args.clone())
            .envs(base_env)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        println!(
            "executed mirrord with args {args:?} pid {}",
            server.id().unwrap()
        );
        // We need to hold temp dir until the process is finished
        TestProcess::from_child(server, temp_dir)
    }

    #[fixture]
    pub async fn kube_client() -> Client {
        let mut config = Config::infer().await.unwrap();
        config.accept_invalid_certs = true;
        Client::try_from(config).unwrap()
    }

    struct ResourceGuard {
        guard: Option<DropGuard>,
        barrier: std::sync::Arc<std::sync::Barrier>,
        handle: JoinHandle<()>,
        delete_on_fail: bool,
    }

    impl ResourceGuard {
        /// Creates a resource and spawns a task to delete it when dropped
        /// I'm not sure why I have to add the `static here but this works?
        pub async fn create<K: Debug + Clone + DeserializeOwned + Serialize + 'static>(
            api: &Api<K>,
            name: String,
            data: &K,
            delete_on_fail: bool,
        ) -> ResourceGuard {
            api.create(&PostParams::default(), data).await.unwrap();
            let cancel_token = CancellationToken::new();
            let resource_token = cancel_token.clone();
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
            let guard_barrier = barrier.clone();
            let name = name.clone();
            let cloned_api = api.clone();
            let handle = tokio::spawn(async move {
                cancel_token.cancelled().await;
                // Don't clean pods on failure, so that we can debug
                println!("deleting {:?}", &name);
                cloned_api
                    .delete(&name, &DeleteParams::default())
                    .await
                    .unwrap();
                barrier.wait();
            });
            Self {
                guard: Some(resource_token.drop_guard()),
                barrier: guard_barrier,
                handle,
                delete_on_fail,
            }
        }
    }

    impl Drop for ResourceGuard {
        fn drop(&mut self) {
            if !self.delete_on_fail && std::thread::panicking() {
                // If we're panicking and we shouldn't delete the resources on fail (to allow for
                // inspection) then abort the cleaning task.
                self.handle.abort();
            } else {
                let guard = self.guard.take();
                drop(guard);
                self.barrier.wait();
            }
        }
    }

    pub struct KubeService {
        name: String,
        namespace: String,
        target: String,
        _pod: ResourceGuard,
        _service: ResourceGuard,
    }

    /// randomize_name: should a random suffix be added to the end of resource names? e.g.
    ///                 for `echo-service`, should we create as `echo-service-ybtdb`.
    /// delete_after_fail: delete resources even if the test fails.
    #[fixture]
    async fn service(
        #[future] kube_client: Client,
        #[default("default")] namespace: &str,
        #[default("NodePort")] service_type: &str,
        #[default("ghcr.io/metalbear-co/mirrord-pytest:latest")] image: &str,
        #[default("http-echo")] service_name: &str,
        #[default(true)] randomize_name: bool,
        #[default(false)] delete_after_fail: bool,
    ) -> KubeService {
        let kube_client = kube_client.await;
        let deployment_api: Api<Deployment> = Api::namespaced(kube_client.clone(), namespace);
        let service_api: Api<Service> = Api::namespaced(kube_client.clone(), namespace);
        let name;
        if randomize_name {
            name = format!("{}-{}", service_name, random_string());
        } else {
            // if using non-random name, delete existing resources first.
            // Just continue if they don't exist.
            let _res = service_api
                .delete(service_name, &DeleteParams::default())
                .await;
            let _res = deployment_api
                .delete(service_name, &DeleteParams::default())
                .await;
            name = service_name.to_string();
        }

        let deployment: Deployment = serde_json::from_value(json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": &name,
                "labels": {
                    "app": &name
                }
            },
            "spec": {
                "replicas": 1,
                "selector": {
                    "matchLabels": {
                        "app": &name
                    }
                },
                "template": {
                    "metadata": {
                        "labels": {
                            "app": &name
                        }
                    },
                    "spec": {
                        "containers": [
                            {
                                "name": &CONTAINER_NAME,
                                "image": &image,
                                "ports": [
                                    {
                                        "containerPort": 80
                                    }
                                ],
                                "env": [
                                    {
                                      "name": "MIRRORD_FAKE_VAR_FIRST",
                                      "value": "mirrord.is.running"
                                    },
                                    {
                                      "name": "MIRRORD_FAKE_VAR_SECOND",
                                      "value": "7777"
                                    },
                                    {
                                        "name": "MIRRORD_FAKE_VAR_THIRD",
                                        "value": "foo=bar"
                                    }
                                ],
                            }
                        ]
                    }
                }
            }
        }))
        .unwrap();
        let pod_guard = ResourceGuard::create(
            &deployment_api,
            name.to_string(),
            &deployment,
            delete_after_fail,
        )
        .await;
        watch_resource_exists(&deployment_api, &name).await;

        let service: Service = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": &name,
                "labels": {
                    "app": &name
                }
            },
            "spec": {
                "type": &service_type,
                "selector": {
                    "app": &name
                },
                "sessionAffinity": "None",
                "ports": [
                    {
                        "name": "udp",
                        "protocol": "UDP",
                        "port": 31415,
                    },
                    {
                        "name": "http",
                        "protocol": "TCP",
                        "port": 80,
                        "targetPort": 80,
                    },
                ]
            }
        }))
        .unwrap();

        let service_guard =
            ResourceGuard::create(&service_api, name.to_string(), &service, delete_after_fail)
                .await;
        watch_resource_exists(&service_api, "default").await;

        let target = get_pod_instance(&kube_client, &name, namespace)
            .await
            .unwrap();
        let pod_api: Api<Pod> = Api::namespaced(kube_client.clone(), namespace);
        await_condition(pod_api, &target, is_pod_running())
            .await
            .unwrap();

        KubeService {
            name: name.to_string(),
            namespace: namespace.to_string(),
            target: format!("pod/{}/container/{}", target, CONTAINER_NAME),
            _pod: pod_guard,
            _service: service_guard,
        }
    }

    /// Service that should only be reachable from inside the cluster, as a communication partner
    /// for testing outgoing traffic. If this service receives the application's messages, they
    /// must have been intercepted and forwarded via the agent to be sent from the impersonated pod.
    #[fixture]
    async fn udp_logger_service(#[future] kube_client: Client) -> KubeService {
        service(
            kube_client,
            "default",
            "ClusterIP",
            "ghcr.io/metalbear-co/mirrord-node-udp-logger:latest",
            "udp-logger",
            true,
            false,
        )
        .await
    }

    #[fixture]
    async fn http_logger_service(#[future] kube_client: Client) -> KubeService {
        service(
            kube_client,
            "default",
            "ClusterIP",
            "ghcr.io/metalbear-co/mirrord-http-logger:latest",
            "mirrord-tests-http-logger",
            false, // So that requester can reach logger by name.
            true,
        )
        .await
    }

    #[fixture]
    async fn http_log_requester_service(#[future] kube_client: Client) -> KubeService {
        service(
            kube_client,
            "default",
            "ClusterIP",
            "ghcr.io/metalbear-co/mirrord-http-log-requester:latest",
            "mirrord-http-log-requester",
            // Have a non-random name, so that there can only be one requester at any point in time
            // so that another requester does not send requests while this one is paused.
            false,
            true, // Delete also on fail, cause this service constantly does work.
        )
        .await
    }

    fn resolve_node_host() -> String {
        if (cfg!(target_os = "linux") && !wsl::is_wsl()) || std::env::var("USE_MINIKUBE").is_ok() {
            let output = std::process::Command::new("minikube")
                .arg("ip")
                .output()
                .unwrap()
                .stdout;
            String::from_utf8_lossy(&output).to_string()
        } else {
            // We assume it's either Docker for Mac or passed via wsl integration
            "127.0.0.1".to_string()
        }
    }

    async fn get_service_url(kube_client: Client, service: &KubeService) -> String {
        let pod_api: Api<Pod> = Api::namespaced(kube_client.clone(), &service.namespace);
        let pods = pod_api
            .list(&ListParams::default().labels(&format!("app={}", service.name)))
            .await
            .unwrap();
        let mut host_ip = pods
            .into_iter()
            .next()
            .and_then(|pod| pod.status)
            .and_then(|status| status.host_ip)
            .unwrap();
        if host_ip.parse::<Ipv4Addr>().unwrap().is_private() {
            host_ip = resolve_node_host();
        }
        let services_api: Api<Service> = Api::namespaced(kube_client.clone(), &service.namespace);
        let services = services_api
            .list(&ListParams::default().labels(&format!("app={}", service.name)))
            .await
            .unwrap();
        let port = services
            .into_iter()
            .next()
            .and_then(|service| service.spec)
            .and_then(|spec| spec.ports)
            .and_then(|mut ports| ports.pop())
            .unwrap();
        format!("http://{}:{}", host_ip, port.node_port.unwrap())
    }

    pub async fn get_pod_instance(
        client: &Client,
        app_name: &str,
        namespace: &str,
    ) -> Option<String> {
        let pod_api: Api<Pod> = Api::namespaced(client.clone(), namespace);
        let pods = pod_api
            .list(&ListParams::default().labels(&format!("app={}", app_name)))
            .await
            .unwrap();
        let pod = pods.iter().next().and_then(|pod| pod.metadata.name.clone());
        pod
    }

    pub async fn send_requests(url: &str, expect_response: bool) {
        // Create client for each request until we have a match between local app and remote app
        // as connection state is flaky
        println!("{url}");
        let client = reqwest::Client::new();
        let res = client.get(url).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        // read all data sent back

        let resp = res.bytes().await.unwrap();
        if expect_response {
            assert_eq!(resp, Bytes::from("GET"));
        }

        let client = reqwest::Client::new();
        let res = client.post(url).body(TEXT).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        // read all data sent back
        let resp = res.bytes().await.unwrap();
        if expect_response {
            assert_eq!(resp, "POST".as_bytes());
        }

        let client = reqwest::Client::new();
        let res = client.put(url).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        // read all data sent back
        let resp = res.bytes().await.unwrap();
        if expect_response {
            assert_eq!(resp, "PUT".as_bytes());
        }

        let client = reqwest::Client::new();
        let res = client.delete(url).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        // read all data sent back
        let resp = res.bytes().await.unwrap();
        if expect_response {
            assert_eq!(resp, "DELETE".as_bytes());
        }
    }

    #[ignore]
    #[cfg(target_os = "linux")]
    #[rstest]
    #[trace]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    async fn test_mirror_http_traffic(
        #[future]
        #[notrace]
        service: KubeService,
        #[future]
        #[notrace]
        kube_client: Client,
        #[values(
            Application::NodeHTTP,
            Application::Go18HTTP,
            Application::Go19HTTP,
            Application::PythonFlaskHTTP,
            Application::PythonFastApiHTTP
        )]
        application: Application,
        #[values(Agent::Ephemeral, Agent::Job)] agent: Agent,
    ) {
        let service = service.await;
        let kube_client = kube_client.await;
        let url = get_service_url(kube_client.clone(), &service).await;
        let mut process = application
            .run(
                &service.target,
                Some(&service.namespace),
                agent.flag(),
                None,
            )
            .await;
        process.wait_for_line(Duration::from_secs(120), "daemon subscribed");
        send_requests(&url, false).await;
        process.wait_for_line(Duration::from_secs(10), "GET");
        process.wait_for_line(Duration::from_secs(10), "POST");
        process.wait_for_line(Duration::from_secs(10), "PUT");
        process.wait_for_line(Duration::from_secs(10), "DELETE");
        timeout(Duration::from_secs(40), process.child.wait())
            .await
            .unwrap()
            .unwrap();

        application.assert(&process);
    }

    #[ignore] // TODO: create integration test instead.
    #[cfg(target_os = "macos")]
    #[rstest]
    #[trace]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    async fn test_mirror_http_traffic(
        #[future]
        #[notrace]
        service: KubeService,
        #[future]
        #[notrace]
        kube_client: Client,
        #[values(Application::PythonFlaskHTTP, Application::PythonFastApiHTTP)]
        application: Application,
        #[values(Agent::Job)] agent: Agent,
    ) {
        let service = service.await;
        let kube_client = kube_client.await;
        let url = get_service_url(kube_client.clone(), &service).await;
        let mut process = application
            .run(
                &service.target,
                Some(&service.namespace),
                agent.flag(),
                None,
            )
            .await;
        process.wait_for_line(Duration::from_secs(300), "daemon subscribed");
        send_requests(&url, false).await;
        process.wait_for_line(Duration::from_secs(10), "GET");
        process.wait_for_line(Duration::from_secs(10), "POST");
        process.wait_for_line(Duration::from_secs(10), "PUT");
        process.wait_for_line(Duration::from_secs(10), "DELETE");
        timeout(Duration::from_secs(40), process.child.wait())
            .await
            .unwrap()
            .unwrap();

        application.assert(&process);
    }

    #[cfg(target_os = "linux")]
    #[rstest]
    #[trace]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    pub async fn test_file_ops(
        #[future]
        #[notrace]
        service: KubeService,
        #[values(Agent::Ephemeral, Agent::Job)] agent: Agent,
        #[values(FileOps::Python, FileOps::Go18, FileOps::Go19, FileOps::Rust)] ops: FileOps,
    ) {
        let service = service.await;
        let _ = std::fs::create_dir(std::path::Path::new("/tmp/fs"));
        let command = ops.command();

        let mut args = vec!["--fs-mode", "write"];

        if let Some(ephemeral_flag) = agent.flag() {
            args.extend(ephemeral_flag);
        }

        let env = vec![("MIRRORD_FILE_READ_WRITE_PATTERN", "/tmp/**")];
        let mut process = run(
            command,
            &service.target,
            Some(&service.namespace),
            Some(args),
            Some(env),
        )
        .await;
        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        ops.assert(process);
    }

    #[cfg(target_os = "macos")]
    #[rstest]
    #[trace]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    pub async fn test_file_ops(
        #[future]
        #[notrace]
        service: KubeService,
        #[values(Agent::Job)] agent: Agent,
    ) {
        let service = service.await;
        let _ = std::fs::create_dir(std::path::Path::new("/tmp/fs"));
        let python_command = vec!["python3", "-B", "-m", "unittest", "-f", "python-e2e/ops.py"];
        let args = vec!["--fs-mode", "read"];
        let env = vec![("MIRRORD_FILE_READ_WRITE_PATTERN", "/tmp/fs/**")];

        let mut process = run(
            python_command,
            &service.target,
            Some(&service.namespace),
            Some(args),
            Some(env),
        )
        .await;
        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_python_fileops_stderr();
    }

    #[rstest]
    #[trace]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    pub async fn test_file_ops_ro(
        #[future]
        #[notrace]
        service: KubeService,
    ) {
        let service = service.await;
        let _ = std::fs::create_dir(std::path::Path::new("/tmp/fs"));
        let python_command = vec![
            "python3",
            "-B",
            "-m",
            "unittest",
            "-f",
            "python-e2e/files_ro.py",
        ];

        let mut process = run(
            python_command,
            &service.target,
            Some(&service.namespace),
            None,
            None,
        )
        .await;
        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_python_fileops_stderr();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    pub async fn test_remote_env_vars_exclude_works(#[future] service: KubeService) {
        let service = service.await;
        let node_command = vec![
            "node",
            "node-e2e/remote_env/test_remote_env_vars_exclude_works.mjs",
        ];
        let mirrord_args = vec!["-x", "MIRRORD_FAKE_VAR_FIRST"];
        let mut process = run(
            node_command,
            &service.target,
            None,
            Some(mirrord_args),
            None,
        )
        .await;

        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    pub async fn test_remote_env_vars_include_works(#[future] service: KubeService) {
        let service = service.await;
        let node_command = vec![
            "node",
            "node-e2e/remote_env/test_remote_env_vars_include_works.mjs",
        ];
        let mirrord_args = vec!["-s", "MIRRORD_FAKE_VAR_FIRST"];
        let mut process = run(
            node_command,
            &service.target,
            None,
            Some(mirrord_args),
            None,
        )
        .await;

        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    pub async fn test_remote_dns_enabled_works(#[future] service: KubeService) {
        let service = service.await;
        let node_command = vec![
            "node",
            "node-e2e/remote_dns/test_remote_dns_enabled_works.mjs",
        ];
        let mut process = run(node_command, &service.target, None, None, None).await;

        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    pub async fn test_remote_dns_lookup_google(#[future] service: KubeService) {
        let service = service.await;
        let node_command = vec![
            "node",
            "node-e2e/remote_dns/test_remote_dns_lookup_google.mjs",
        ];
        let mut process = run(node_command, &service.target, None, None, None).await;

        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    #[cfg(target_os = "linux")]
    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    async fn test_steal_http_traffic(
        #[future] service: KubeService,
        #[future] kube_client: Client,
        #[values(
            Application::PythonFlaskHTTP,
            Application::PythonFastApiHTTP,
            Application::NodeHTTP
        )]
        application: Application,
        #[values(Agent::Ephemeral, Agent::Job)] agent: Agent,
    ) {
        let service = service.await;
        let kube_client = kube_client.await;
        let url = get_service_url(kube_client.clone(), &service).await;
        let mut flags = vec!["--steal"];
        agent.flag().map(|flag| flags.extend(flag));
        let mut process = application
            .run(&service.target, Some(&service.namespace), Some(flags), None)
            .await;

        process.wait_for_line(Duration::from_secs(40), "daemon subscribed");
        send_requests(&url, true).await;
        timeout(Duration::from_secs(40), process.child.wait())
            .await
            .unwrap()
            .unwrap();

        application.assert(&process);
    }

    #[rstest]
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    pub async fn test_bash_remote_env_vars_works(#[future] service: KubeService) {
        let service = service.await;
        let bash_command = vec!["bash", "bash-e2e/env.sh"];
        let mut process = run(bash_command, &service.target, None, None, None).await;

        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    #[rstest]
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    pub async fn test_bash_remote_env_vars_exclude_works(#[future] service: KubeService) {
        let service = service.await;
        let bash_command = vec!["bash", "bash-e2e/env.sh", "exclude"];
        let mirrord_args = vec!["-x", "MIRRORD_FAKE_VAR_FIRST"];
        let mut process = run(
            bash_command,
            &service.target,
            None,
            Some(mirrord_args),
            None,
        )
        .await;

        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    #[rstest]
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    pub async fn test_bash_remote_env_vars_include_works(#[future] service: KubeService) {
        let service = service.await;
        let bash_command = vec!["bash", "bash-e2e/env.sh", "include"];
        let mirrord_args = vec!["-s", "MIRRORD_FAKE_VAR_FIRST"];
        let mut process = run(
            bash_command,
            &service.target,
            None,
            Some(mirrord_args),
            None,
        )
        .await;

        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    // Currently fails due to Layer >> AddressConversion in ci for some reason

    #[ignore]
    #[cfg(target_os = "linux")]
    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    pub async fn test_bash_file_exists(#[future] service: KubeService) {
        let service = service.await;
        let bash_command = vec!["bash", "bash-e2e/file.sh", "exists"];
        let mut process = run(bash_command, &service.target, None, None, None).await;

        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    // currently there is an issue with piping across forks of processes so 'test_bash_file_read'
    // and 'test_bash_file_write' cannot pass

    #[ignore]
    #[cfg(target_os = "linux")]
    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    pub async fn test_bash_file_read(#[future] service: KubeService) {
        let service = service.await;
        let bash_command = vec!["bash", "bash-e2e/file.sh", "read"];
        let mut process = run(bash_command, &service.target, None, None, None).await;

        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    #[ignore]
    #[cfg(target_os = "linux")]
    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    pub async fn test_bash_file_write(#[future] service: KubeService) {
        let service = service.await;
        let bash_command = vec!["bash", "bash-e2e/file.sh", "write"];
        let args = vec!["--rw"];
        let mut process = run(bash_command, &service.target, None, Some(args), None).await;

        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_go18_remote_env_vars_works(#[future] service: KubeService) {
        let service = service.await;
        let command = vec!["go-e2e-env/18"];
        let mut process = run(command, &service.target, None, None, None).await;
        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_go19_remote_env_vars_works(#[future] service: KubeService) {
        let service = service.await;
        let command = vec!["go-e2e-env/19"];
        let mut process = run(command, &service.target, None, None, None).await;
        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    // TODO: change outgoing TCP tests to use the same setup as in the outgoing UDP test so that
    //       they actually verify that the traffic is intercepted and forwarded (and isn't just
    //       directly sent out from the local application).
    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_outgoing_traffic_single_request_enabled(#[future] service: KubeService) {
        let service = service.await;
        let node_command = vec![
            "node",
            "node-e2e/outgoing/test_outgoing_traffic_single_request.mjs",
        ];
        let mut process = run(node_command, &service.target, None, None, None).await;

        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[should_panic]
    pub async fn test_outgoing_traffic_single_request_ipv6(#[future] service: KubeService) {
        let service = service.await;
        let node_command = vec![
            "node",
            "node-e2e/outgoing/test_outgoing_traffic_single_request_ipv6.mjs",
        ];
        let mut process = run(node_command, &service.target, None, None, None).await;

        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_outgoing_traffic_single_request_disabled(#[future] service: KubeService) {
        let service = service.await;
        let node_command = vec![
            "node",
            "node-e2e/outgoing/test_outgoing_traffic_single_request.mjs",
        ];
        let mirrord_args = vec!["--no-outgoing"];
        let mut process = run(
            node_command,
            &service.target,
            None,
            Some(mirrord_args),
            None,
        )
        .await;

        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_outgoing_traffic_many_requests_enabled(#[future] service: KubeService) {
        let service = service.await;
        let node_command = vec![
            "node",
            "node-e2e/outgoing/test_outgoing_traffic_many_requests.mjs",
        ];
        let mut process = run(node_command, &service.target, None, None, None).await;

        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_outgoing_traffic_many_requests_disabled(#[future] service: KubeService) {
        let service = service.await;
        let node_command = vec![
            "node",
            "node-e2e/outgoing/test_outgoing_traffic_many_requests.mjs",
        ];
        let mirrord_args = vec!["--no-outgoing"];
        let mut process = run(
            node_command,
            &service.target,
            None,
            Some(mirrord_args),
            None,
        )
        .await;

        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_outgoing_traffic_make_request_after_listen(#[future] service: KubeService) {
        let service = service.await;
        let node_command = vec![
            "node",
            "node-e2e/outgoing/test_outgoing_traffic_make_request_after_listen.mjs",
        ];
        let mut process = run(node_command, &service.target, None, None, None).await;
        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_outgoing_traffic_make_request_localhost(#[future] service: KubeService) {
        let service = service.await;
        let node_command = vec![
            "node",
            "node-e2e/outgoing/test_outgoing_traffic_make_request_localhost.mjs",
        ];
        let mut process = run(node_command, &service.target, None, None, None).await;
        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    /// Currently, mirrord only intercepts and forwards outgoing udp traffic if the application
    /// binds a non-0 port and calls `connect`. This test runs with mirrord a node app that does
    /// that and verifies that mirrord intercepts and forwards the outgoing udp message.
    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    pub async fn test_outgoing_traffic_udp_with_connect(
        #[future] udp_logger_service: KubeService,
        #[future] service: KubeService,
        #[future] kube_client: Client,
    ) {
        let internal_service = udp_logger_service.await; // Only reachable from withing the cluster.
        let target_service = service.await; // Impersonate a pod of this service, to reach internal.
        let kube_client = kube_client.await;
        let pod_api: Api<Pod> = Api::namespaced(kube_client.clone(), &internal_service.namespace);
        let mut lp = LogParams {
            container: Some(String::from(CONTAINER_NAME)),
            follow: false,
            limit_bytes: None,
            pretty: false,
            previous: false,
            since_seconds: None,
            tail_lines: None,
            timestamps: false,
        };

        let node_command = vec![
            "node",
            "node-e2e/outgoing/test_outgoing_traffic_udp_client_with_connect.mjs",
            "31415",
            // Reaching service by only service name is only possible from within the cluster.
            &internal_service.name,
        ];

        // Meta-test: verify that the application cannot reach the internal service without
        // mirrord forwarding outgoing UDP traffic via the target pod.
        // If this verification fails, the test itself is invalid.
        let mirrord_no_outgoing = vec!["--no-outgoing"];
        let mut process = run(
            node_command.clone(),
            &target_service.target,
            Some(&target_service.namespace),
            Some(mirrord_no_outgoing),
            None,
        )
        .await;
        let res = process.child.wait().await.unwrap();
        assert!(!res.success()); // Should fail because local process cannot reach service.
        let stripped_target = internal_service.target.split('/').collect::<Vec<&str>>()[1];
        let logs = pod_api.logs(stripped_target, &lp).await;
        assert_eq!(logs.unwrap(), "");

        // Run mirrord with outgoing enabled.
        let mut process = run(
            node_command,
            &target_service.target,
            Some(&target_service.namespace),
            None,
            None,
        )
        .await;
        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();

        // Verify that the UDP message sent by the application reached the internal service.
        lp.follow = true; // Follow log stream.
        let logs = pod_api
            .log_stream(stripped_target, &lp)
            .await
            .unwrap()
            .try_next()
            .await
            .unwrap()
            .unwrap();
        let logs = String::from_utf8_lossy(&logs);
        assert!(logs.contains("Can I pass the test please?")); // Of course you can.
    }

    /// Test that the process does not crash and messages are sent out normally when the
    /// application calls `connect` on a UDP socket with outgoing traffic disabled on mirrord.
    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(30))]
    pub async fn test_outgoing_disabled_udp(#[future] service: KubeService) {
        let service = service.await;
        // Binding specific port, because if we bind 0 then we get a  port that is bypassed by
        // mirrord and then the tested crash is not prevented by the fix but by the bypassed port.
        let socket = UdpSocket::bind("127.0.0.1:31415").unwrap();
        let port = socket.local_addr().unwrap().port().to_string();

        let node_command = vec![
            "node",
            "node-e2e/outgoing/test_outgoing_traffic_udp_client_with_connect.mjs",
            &port,
        ];
        let mirrord_args = vec!["--no-outgoing"];
        let mut process = run(
            node_command,
            &service.target,
            None,
            Some(mirrord_args),
            None,
        )
        .await;

        // Listen for UDP message directly from application.
        let mut buf = [0; 27];
        let amt = socket.recv(&mut buf).unwrap();
        assert_eq!(amt, 27);
        assert_eq!(buf, "Can I pass the test please?".as_ref()); // Sure you can.

        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    pub async fn test_go(service: impl Future<Output = KubeService>, command: Vec<&str>) {
        let service = service.await;
        let mut process = run(command, &service.target, None, None, None).await;
        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_go18_outgoing_traffic_single_request_enabled(#[future] service: KubeService) {
        let command = vec!["go-e2e-outgoing/18"];
        test_go(service, command).await;
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_go19_outgoing_traffic_single_request_enabled(#[future] service: KubeService) {
        let command = vec!["go-e2e-outgoing/19"];
        test_go(service, command).await;
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_go18_dns_lookup(#[future] service: KubeService) {
        let command = vec!["go-e2e-dns/18"];
        test_go(service, command).await;
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_go19_dns_lookup(#[future] service: KubeService) {
        let command = vec!["go-e2e-dns/19"];
        test_go(service, command).await;
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_listen_localhost(#[future] service: KubeService) {
        let service = service.await;
        let node_command = vec!["node", "node-e2e/listen/test_listen_localhost.mjs"];
        let mut process = run(node_command, &service.target, None, None, None).await;
        let res = process.child.wait().await.unwrap();
        assert!(res.success());
        process.assert_stderr();
    }

    async fn get_next_log<T: Stream<Item = Result<Bytes, kube::Error>> + Unpin>(
        stream: &mut T,
    ) -> String {
        String::from_utf8_lossy(&stream.try_next().await.unwrap().unwrap()).to_string()
    }

    /// http_logger_service is a service that logs strings from the uri of incoming http requests.
    /// http_log_requester is a service that repeatedly sends a string over requests to the logger.
    /// Deploy the services, the requester sends requests to the logger.
    /// Run a requester with a different string with mirrord with --pause.
    /// verify that the stdout of the logger looks like:
    ///
    /// <string-from-deployed-requester>
    /// <string-from-deployed-requester>
    ///              ...
    /// <string-from-deployed-requester>
    /// <string-from-mirrord-requester>
    /// <string-from-mirrord-requester>
    /// <string-from-deployed-requester>
    /// <string-from-deployed-requester>
    ///              ...
    /// <string-from-deployed-requester>
    ///
    /// Which means the deployed requester was indeed paused while the local requester was running
    /// with mirrord, because local requester waits between its two requests enough time for the
    /// deployed requester to send more requests it were not paused.
    ///
    /// To run on mac, first build universal binary: (from repo root) `scripts/build_fat_mac.sh`
    /// then run test with MIRRORD_TESTS_USE_BINARY=../target/universal-apple-darwin/debug/mirrord
    /// Because the test runs a bash script with mirrord and that requires the universal binary.
    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    pub async fn pause_log_requests(
        #[future] http_logger_service: KubeService,
        #[future] http_log_requester_service: KubeService,
        #[future] kube_client: Client,
    ) {
        let logger_service = http_logger_service.await;
        let requester_service = http_log_requester_service.await; // Impersonate a pod of this service, to reach internal.
        let kube_client = kube_client.await;
        let pod_api: Api<Pod> = Api::namespaced(kube_client.clone(), &logger_service.namespace);

        let target_parts = logger_service.target.split('/').collect::<Vec<&str>>();
        let pod_name = target_parts[1];
        let container_name = target_parts[3];
        let lp = LogParams {
            container: Some(container_name.to_string()), // Default to first, we only have 1.
            follow: true,
            limit_bytes: None,
            pretty: false,
            previous: false,
            since_seconds: None,
            tail_lines: None,
            timestamps: false,
        };

        println!("getting log stream.");
        let log_stream = pod_api.log_stream(pod_name, &lp).await.unwrap();

        let command = vec!["pause/send_reqs.sh"];

        let mirrord_pause_arg = vec!["--pause"];

        println!("Waiting for 2 flask lines.");
        let mut log_stream = log_stream.skip(2); // Skip flask prints.

        let hi_from_deployed_app = "hi-from-deployed-app\n";
        let hi_from_local_app = "hi-from-local-app\n";
        let hi_again_from_local_app = "hi-again-from-local-app\n";

        println!("Waiting for first log by deployed app.");
        let first_log = get_next_log(&mut log_stream).await;

        assert_eq!(first_log, hi_from_deployed_app);

        println!("Running local app with mirrord.");
        let mut process = run(
            command.clone(),
            &requester_service.target,
            Some(&requester_service.namespace),
            Some(mirrord_pause_arg),
            None,
        )
        .await;
        let res = process.child.wait().await.unwrap();
        println!("mirrord done running.");
        assert!(res.success());

        println!("Spooling logs forward to get to local app's first log.");
        // Skip all the logs by the deployed app from before we ran local.
        let mut next_log = get_next_log(&mut log_stream).await;
        while next_log == hi_from_deployed_app {
            next_log = get_next_log(&mut log_stream).await;
        }

        // Verify first log from local app.
        assert_eq!(next_log, hi_from_local_app);

        // Verify that the second log from local app comes right after it - the deployed requester
        // was paused.
        let log_from_local = get_next_log(&mut log_stream).await;
        assert_eq!(log_from_local, hi_again_from_local_app);

        // Verify that the deployed app resumes after the local app is done.
        let log_from_deployed_after_resume = get_next_log(&mut log_stream).await;
        assert_eq!(log_from_deployed_after_resume, hi_from_deployed_app);
    }
}
