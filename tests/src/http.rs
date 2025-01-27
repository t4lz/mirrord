#[cfg(test)]
mod http_tests {

    use std::time::Duration;

    use k8s_openapi::api::core::v1::Pod;
    use kube::{Api, Client};
    use rstest::*;
    use tokio::time::timeout;

    use crate::utils::{
        get_service_url,
        ipv6::{ipv6_service, portforward_http_requests},
        kube_client, send_requests, service, Application, KubeService,
    };

    /// ## Warning
    ///
    /// These tests are marked with `ignore` due to flakyness!
    #[ignore]
    #[cfg(target_os = "linux")]
    #[rstest]
    #[trace]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    async fn mirror_http_traffic(
        #[future]
        #[notrace]
        service: KubeService,
        #[future]
        #[notrace]
        kube_client: Client,
        #[values(
            Application::NodeHTTP,
            Application::Go21HTTP,
            Application::Go22HTTP,
            Application::Go23HTTP,
            Application::PythonFlaskHTTP,
            Application::PythonFastApiHTTP
        )]
        application: Application,
    ) {
        let service = service.await;
        let kube_client = kube_client.await;
        let url = get_service_url(kube_client.clone(), &service).await;
        let mut process = application
            .run(
                &service.pod_container_target(),
                Some(&service.namespace),
                None,
                None,
            )
            .await;
        process
            .wait_for_line(Duration::from_secs(120), "daemon subscribed")
            .await;
        send_requests(&url, false, Default::default()).await;
        process.wait_for_line(Duration::from_secs(10), "GET").await;
        process.wait_for_line(Duration::from_secs(10), "POST").await;
        process.wait_for_line(Duration::from_secs(10), "PUT").await;
        process
            .wait_for_line(Duration::from_secs(10), "DELETE")
            .await;
        timeout(Duration::from_secs(40), process.wait())
            .await
            .unwrap();

        application.assert(&process).await;
    }

    #[ignore] // TODO: create integration test instead.
    #[cfg(target_os = "macos")]
    #[rstest]
    #[trace]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    async fn mirror_http_traffic(
        #[future]
        #[notrace]
        service: KubeService,
        #[future]
        #[notrace]
        kube_client: Client,
        #[values(Application::PythonFlaskHTTP, Application::PythonFastApiHTTP)]
        application: Application,
    ) {
        let service = service.await;
        let kube_client = kube_client.await;
        let url = get_service_url(kube_client.clone(), &service).await;
        let mut process = application
            .run(
                &service.pod_container_target(),
                Some(&service.namespace),
                None,
                None,
            )
            .await;
        process
            .wait_for_line(Duration::from_secs(300), "daemon subscribed")
            .await;
        send_requests(&url, false, Default::default()).await;
        process.wait_for_line(Duration::from_secs(10), "GET").await;
        process.wait_for_line(Duration::from_secs(10), "POST").await;
        process.wait_for_line(Duration::from_secs(10), "PUT").await;
        process
            .wait_for_line(Duration::from_secs(10), "DELETE")
            .await;
        timeout(Duration::from_secs(40), process.wait())
            .await
            .unwrap();

        application.assert(&process).await;
    }

    #[ignore]
    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[timeout(Duration::from_secs(240))]
    async fn mirror_ipv6_http_traffic(
        #[future] ipv6_service: KubeService,
        #[future] kube_client: Client,
    ) {
        let application = Application::PythonFastApiHTTPIPv6;
        let service = ipv6_service.await;
        let kube_client = kube_client.await;

        let mut process = application
            .run(
                &service.pod_container_target(),
                Some(&service.namespace),
                None,
                Some(vec![("MIRRORD_ENABLE_IPV6", "true")]),
            )
            .await;
        process
            .wait_for_line(Duration::from_secs(40), "daemon subscribed")
            .await;

        let api = Api::<Pod>::namespaced(kube_client.clone(), &service.namespace);
        portforward_http_requests(&api, service, false).await;

        // Verify the local app running with mirrord gets the incoming requests.
        for method in ["GET", "POST", "PUT", "DELETE"] {
            process.wait_for_line(Duration::from_secs(10), method).await;
        }

        tokio::time::timeout(Duration::from_secs(40), process.wait())
            .await
            .unwrap();

        application.assert(&process).await;
    }
}
