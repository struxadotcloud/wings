use futures::StreamExt;
use rand::distr::SampleString;
use std::{
    collections::HashMap,
    path::Path,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncWriteExt, ReadBuf},
    sync::RwLock,
};

#[inline]
pub fn string_to_option(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

#[async_trait::async_trait]
trait DockerServerConfigurationExt {
    async fn convert_mounts(
        &self,
        config: &crate::config::Config,
        filesystem: &crate::server::filesystem::Filesystem,
    ) -> Vec<bollard::plugin::Mount>;

    #[cfg(unix)]
    fn convert_devices(&self) -> Vec<bollard::models::DeviceMapping>;

    fn convert_allocations_bindings(&self) -> bollard::models::PortMap;
    fn convert_allocations_docker_bindings(
        &self,
        config: &crate::config::Config,
    ) -> bollard::models::PortMap;
    fn convert_allocations_exposed(&self) -> Vec<String>;

    async fn container_config(
        &self,
        config: &crate::config::Config,
        client: &bollard::Docker,
        filesystem: &crate::server::filesystem::Filesystem,
    ) -> bollard::plugin::ContainerCreateBody;
    fn container_update_config(
        &self,
        config: &crate::config::Config,
    ) -> bollard::plugin::ContainerUpdateBody;

    fn installer_resources(&self, config: &crate::config::Config) -> bollard::models::Resources;
}

#[async_trait::async_trait]
impl DockerServerConfigurationExt for crate::server::configuration::ServerConfiguration {
    async fn convert_mounts(
        &self,
        config: &crate::config::Config,
        filesystem: &crate::server::filesystem::Filesystem,
    ) -> Vec<bollard::models::Mount> {
        self.mounts(config, filesystem)
            .await
            .into_iter()
            .map(|mount| bollard::models::Mount {
                typ: Some(bollard::plugin::MountType::BIND),
                target: Some(mount.target.into()),
                source: Some(mount.source.into()),
                read_only: Some(mount.read_only),
                ..Default::default()
            })
            .collect()
    }

    #[cfg(unix)]
    fn convert_devices(&self) -> Vec<bollard::models::DeviceMapping> {
        let mut devices = Vec::new();

        if self.container.kvm_passthrough_enabled {
            devices.push(bollard::models::DeviceMapping {
                path_on_host: Some("/dev/kvm".into()),
                path_in_container: Some("/dev/kvm".into()),
                cgroup_permissions: Some("rwm".into()),
            });
        }

        devices
    }

    fn convert_allocations_bindings(&self) -> bollard::models::PortMap {
        let mut map = HashMap::new();

        for (ip, ports) in &self.allocations.mappings {
            for port in ports {
                let binding = bollard::models::PortBinding {
                    host_ip: Some(ip.to_string()),
                    host_port: Some(port.to_string()),
                };

                if let Some(tcp_bindings) = map
                    .entry(format!("{port}/tcp"))
                    .or_insert_with(|| Some(Vec::new()))
                {
                    tcp_bindings.push(binding.clone());
                }

                if let Some(udp_bindings) = map
                    .entry(format!("{port}/udp"))
                    .or_insert_with(|| Some(Vec::new()))
                {
                    udp_bindings.push(binding);
                }
            }
        }

        map
    }

    fn convert_allocations_docker_bindings(
        &self,
        config: &crate::config::Config,
    ) -> bollard::models::PortMap {
        let config = config.load();
        let iface = &config.docker.network.interface;
        let mut map = self.convert_allocations_bindings();

        for (_port, binds_option) in map.iter_mut() {
            if let Some(binds) = binds_option {
                let mut i = 0;
                while i < binds.len() {
                    if config.docker.network.disable_interface_binding {
                        binds[i].host_ip = None;
                    }

                    if binds[i].host_ip.as_deref() == Some("127.0.0.1") {
                        if config.docker.network.ispn {
                            binds.remove(i);

                            continue;
                        } else {
                            binds[i].host_ip = Some(iface.clone());
                        }
                    }

                    i += 1;
                }
            }
        }

        map
    }

    fn convert_allocations_exposed(&self) -> Vec<String> {
        let mut exposed = Vec::new();

        for ports in self.allocations.mappings.values() {
            for port in ports {
                exposed.push(format!("{port}/tcp"));
                exposed.push(format!("{port}/udp"));
            }
        }

        exposed
    }

    async fn container_config(
        &self,
        config: &crate::config::Config,
        client: &bollard::Docker,
        filesystem: &crate::server::filesystem::Filesystem,
    ) -> bollard::plugin::ContainerCreateBody {
        let mut labels = self.labels.clone();
        labels.insert("Service".into(), config.load().app_name.clone());
        labels.insert("ContainerType".into(), "server_process".into());

        let network_mode = if self.allocations.force_outgoing_ip
            && let Some(default) = &self.allocations.default
        {
            let network_name = format!("ip-{}", default.ip.replace('.', "-").replace(':', "--"));

            if client.inspect_network(&network_name, None).await.is_err()
                && let Err(err) = client
                    .create_network(bollard::plugin::NetworkCreateRequest {
                        name: network_name.to_string(),
                        driver: Some("bridge".to_string()),
                        enable_ipv6: Some(false),
                        internal: Some(false),
                        attachable: Some(false),
                        ingress: Some(false),
                        options: Some(HashMap::from([
                            ("encryption".to_string(), "false".to_string()),
                            (
                                "com.docker.network.bridge.default_bridge".to_string(),
                                "false".to_string(),
                            ),
                            (
                                "com.docker.network.host_ipv4".to_string(),
                                default.ip.to_string(),
                            ),
                        ])),
                        ..Default::default()
                    })
                    .await
            {
                tracing::error!(
                    server = %self.uuid,
                    "failed to create container network {}: {}",
                    network_name,
                    err
                );
            }

            network_name
        } else {
            config.load().docker.network.mode.clone()
        };

        let mut resources = self.convert_container_resources(config);
        resources.blkio_weight = None; // blkio_weight is cgroup v1 only; fails on cgroup v2

        bollard::plugin::ContainerCreateBody {
            exposed_ports: Some(self.convert_allocations_exposed()),
            host_config: Some(bollard::plugin::HostConfig {
                memory: resources.memory,
                memory_reservation: resources.memory_reservation,
                memory_swap: resources.memory_swap,
                cpu_quota: resources.cpu_quota,
                cpu_period: resources.cpu_period,
                cpu_shares: resources.cpu_shares,
                cpuset_cpus: resources.cpuset_cpus,
                pids_limit: resources.pids_limit,
                blkio_weight: resources.blkio_weight,
                oom_kill_disable: resources.oom_kill_disable,

                port_bindings: Some(self.convert_allocations_docker_bindings(config)),
                mounts: Some(self.convert_mounts(config, filesystem).await),
                #[cfg(unix)]
                devices: Some(self.convert_devices()),
                network_mode: Some(network_mode),
                dns: Some(config.load().docker.network.dns.clone()),
                tmpfs: Some(HashMap::from([(
                    "/tmp".to_string(),
                    format!("rw,exec,nosuid,size={}M", config.load().docker.tmpfs_size),
                )])),
                log_config: Some(bollard::plugin::HostConfigLogConfig {
                    typ: Some(config.load().docker.log_config.r#type.clone()),
                    config: Some(
                        config
                            .load()
                            .docker
                            .log_config
                            .config
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect(),
                    ),
                }),
                security_opt: Some(vec![
                    "no-new-privileges".to_string(),
                    crate::server::configuration::seccomp::Seccomp::default()
                        .remove_names(
                            &self.container.seccomp.remove_allowed,
                            crate::server::configuration::seccomp::Action::Allow,
                        )
                        .to_string()
                        .unwrap(),
                ]),
                cap_drop: Some(vec![
                    "setpcap".to_string(),
                    "mknod".to_string(),
                    "audit_write".to_string(),
                    "net_raw".to_string(),
                    "dac_override".to_string(),
                    "fowner".to_string(),
                    "fsetid".to_string(),
                    "net_bind_service".to_string(),
                    "sys_chroot".to_string(),
                    "setfcap".to_string(),
                    "sys_ptrace".to_string(),
                ]),
                userns_mode: string_to_option(&config.load().docker.userns_mode),
                readonly_rootfs: Some(true),
                ..Default::default()
            }),
            hostname: Some(self.uuid.to_string()),
            domainname: string_to_option(&config.load().docker.domainname),
            entrypoint: self.entrypoint.clone(),
            image: Some(self.container.image.trim_end_matches('~').to_string()),
            env: Some(self.environment(config)),
            user: Some(if config.load().system.user.rootless.enabled {
                let config = config.load();

                format!(
                    "{}:{}",
                    config.system.user.rootless.container_uid,
                    config.system.user.rootless.container_gid
                )
            } else {
                let config = config.load();

                format!("{}:{}", config.system.user.uid, config.system.user.gid)
            }),
            labels: Some(labels),
            attach_stdin: Some(true),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            open_stdin: Some(true),
            tty: Some(true),
            ..Default::default()
        }
    }

    fn container_update_config(
        &self,
        config: &crate::config::Config,
    ) -> bollard::plugin::ContainerUpdateBody {
        let resources = self.convert_container_resources(config);

        bollard::plugin::ContainerUpdateBody {
            memory: resources.memory,
            memory_reservation: resources.memory_reservation,
            memory_swap: resources.memory_swap,
            cpu_quota: resources.cpu_quota,
            cpu_period: resources.cpu_period,
            cpu_shares: resources.cpu_shares,
            cpuset_cpus: resources.cpuset_cpus,
            pids_limit: resources.pids_limit,
            blkio_weight: None, // blkio_weight is cgroup v1 only; fails on cgroup v2
            oom_kill_disable: resources.oom_kill_disable,
            ..Default::default()
        }
    }

    fn installer_resources(&self, config: &crate::config::Config) -> bollard::models::Resources {
        let mut resources = self.convert_container_resources(config);

        let config = config.load();
        let installer_limits = &config.docker.installer_limits;

        if resources
            .memory_reservation
            .is_some_and(|m| m > 0 && m < installer_limits.memory.as_bytes() as i64)
        {
            resources.memory = None;
            resources.memory_reservation = Some(installer_limits.memory.as_bytes() as i64);
            resources.memory_swap = None;
        }

        if resources
            .cpu_quota
            .is_some_and(|c| c > 0 && c < installer_limits.cpu as i64 * 1000)
        {
            resources.cpu_quota = Some(installer_limits.cpu as i64 * 1000);
        }

        resources
    }
}

pub struct DockerExecutor {
    docker: Arc<bollard::Docker>,
    app_config: Arc<crate::config::Config>,
}

impl DockerExecutor {
    pub fn new(docker: Arc<bollard::Docker>, app_config: Arc<crate::config::Config>) -> Self {
        Self { docker, app_config }
    }

    async fn pull_image(
        &self,
        image: &str,
        server: &super::super::Server,
        quiet: bool,
    ) -> Result<(), anyhow::Error> {
        if image.ends_with('~') {
            return Ok(());
        }

        if !quiet {
            server.log_daemon_with_prelude(
                "Pulling Docker container image, this could take a few minutes to complete...",
            );
        }

        let mut registry_auth = None;
        for (registry, config) in self.app_config.load().docker.registries.iter() {
            if image.starts_with(registry.as_str()) {
                registry_auth = Some(bollard::auth::DockerCredentials {
                    username: Some(config.username.clone()),
                    password: Some(config.password.clone()),
                    serveraddress: Some(registry.clone()),
                    ..Default::default()
                });
                break;
            }
        }

        let (image_name, tag) = image.split_once(':').unwrap_or((image, "latest"));

        let mut stream = self.docker.create_image(
            Some(bollard::query_parameters::CreateImageOptions {
                from_image: Some(image_name.to_string()),
                tag: Some(tag.to_string()),
                ..Default::default()
            }),
            None,
            registry_auth,
        );

        while let Some(status) = stream.next().await {
            match status {
                Ok(info) => {
                    if let Some(id) = &info.id {
                        match info.status.as_deref().map(str::to_lowercase).as_deref() {
                            Some("downloading") => {
                                if let Some(ref detail) = info.progress_detail {
                                    server
                                        .websocket
                                        .send(super::super::websocket::WebsocketMessage::new(
                                            super::super::websocket::WebsocketEvent::ServerImagePullProgress,
                                            [
                                                id.clone().into(),
                                                serde_json::to_string(&crate::models::PullProgress {
                                                    status: crate::models::PullProgressStatus::Pulling,
                                                    progress: detail.current.unwrap_or_default(),
                                                    total: detail.total.unwrap_or_default(),
                                                })
                                                .unwrap()
                                                .into(),
                                            ]
                                            .into(),
                                        ))
                                        .ok();
                                }
                            }
                            Some("extracting") => {
                                if let Some(ref detail) = info.progress_detail {
                                    server
                                        .websocket
                                        .send(super::super::websocket::WebsocketMessage::new(
                                            super::super::websocket::WebsocketEvent::ServerImagePullProgress,
                                            [
                                                id.clone().into(),
                                                serde_json::to_string(&crate::models::PullProgress {
                                                    status: crate::models::PullProgressStatus::Extracting,
                                                    progress: detail.current.unwrap_or_default(),
                                                    total: detail.total.unwrap_or_default(),
                                                })
                                                .unwrap()
                                                .into(),
                                            ]
                                            .into(),
                                        ))
                                        .ok();
                                }
                            }
                            Some("pull complete") => {
                                server
                                    .websocket
                                    .send(super::super::websocket::WebsocketMessage::new(
                                        super::super::websocket::WebsocketEvent::ServerImagePullCompleted,
                                        [id.clone().into()].into(),
                                    ))
                                    .ok();
                            }
                            _ => {}
                        }
                    }

                    if !quiet && let Some(status_str) = info.status {
                        if let Some(ref detail) = info.progress_detail {
                            server.log_daemon_install(
                                format!(
                                    "{status_str} {} of {}",
                                    crate::utils::draw_progress_bar(
                                        50usize.saturating_sub(status_str.len()),
                                        detail.current.unwrap_or_default() as f64,
                                        detail.total.unwrap_or_default() as f64,
                                    ),
                                    human_bytes::human_bytes(
                                        detail.total.unwrap_or_default() as f64
                                    ),
                                )
                                .into(),
                            );
                        } else {
                            server.log_daemon_install(status_str.into());
                        }
                    }
                }
                Err(err) => {
                    tracing::error!(
                        server = %server.uuid,
                        image = %image_name,
                        "failed to pull image: {:?}",
                        err
                    );

                    if !quiet {
                        server.log_daemon_error(&format!("failed to pull image: {err}"));
                    }

                    let exists = self
                        .docker
                        .list_images(Some(bollard::query_parameters::ListImagesOptions {
                            all: true,
                            filters: Some(HashMap::from([(
                                "reference".to_string(),
                                vec![image_name.to_string()],
                            )])),
                            ..Default::default()
                        }))
                        .await
                        .is_ok_and(|images| !images.is_empty());

                    if !exists {
                        return Err(err.into());
                    }

                    tracing::warn!(
                        server = %server.uuid,
                        image = %image_name,
                        "image already exists locally, ignoring pull error"
                    );
                }
            }
        }

        if !quiet {
            server.log_daemon_with_prelude("Finished pulling Docker container image");
        }

        Ok(())
    }
}

struct LogsReader {
    stream: futures::stream::BoxStream<'static, Result<Vec<u8>, std::io::Error>>,
    buffer: Vec<u8>,
    pos: usize,
}

impl tokio::io::AsyncRead for LogsReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        loop {
            if self.pos < self.buffer.len() {
                let n = buf.remaining().min(self.buffer.len() - self.pos);
                buf.put_slice(&self.buffer[self.pos..self.pos + n]);
                self.pos += n;
                return Poll::Ready(Ok(()));
            }

            self.buffer.clear();
            self.pos = 0;

            match self.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => self.buffer = chunk,
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

struct DockerProcessHandle {
    container_id: String,
    docker: Arc<bollard::Docker>,
    server: Weak<super::super::InnerServer>,
    app_config: Arc<crate::config::Config>,

    resource_usage: Arc<RwLock<super::super::resources::ResourceUsage>>,
    stdin_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    stdout_rx: tokio::sync::broadcast::Receiver<Arc<compact_str::CompactString>>,

    state_task: tokio::task::JoinHandle<()>,
    stats_task: tokio::task::JoinHandle<()>,
    stdin_task: tokio::task::JoinHandle<()>,
    stdout_task: tokio::task::JoinHandle<()>,
}

impl DockerProcessHandle {
    async fn new(
        container_id: String,
        docker: Arc<bollard::Docker>,
        server: &super::super::Server,
        app_config: Arc<crate::config::Config>,
        status_tx: tokio::sync::mpsc::Sender<(
            super::ProcessStatus,
            super::super::resources::ResourceUsage,
        )>,
    ) -> Result<Self, anyhow::Error> {
        let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(150);
        let (stdout_tx, stdout_rx) =
            tokio::sync::broadcast::channel::<Arc<compact_str::CompactString>>(150);

        let resource_usage = Arc::new(RwLock::new(super::super::resources::ResourceUsage {
            disk_bytes: server.filesystem.limiter_usage().await,
            state: server.state.get_state(),
            ..Default::default()
        }));

        tracing::debug!(container_id = %container_id, "DockerProcessHandle::new: attaching to container");
        let mut attach = match docker
            .attach_container(
                &container_id,
                Some(bollard::query_parameters::AttachContainerOptions {
                    stdin: true,
                    stdout: true,
                    stderr: true,
                    stream: true,
                    ..Default::default()
                }),
            )
            .await
        {
            Ok(a) => {
                tracing::debug!(container_id = %container_id, "DockerProcessHandle::new: attached to container");
                a
            }
            Err(err) => {
                tracing::error!(container_id = %container_id, error = %err, "DockerProcessHandle::new: failed to attach to container");
                return Err(err.into());
            }
        };

        let stdin_task = tokio::spawn(async move {
            while let Some(data) = stdin_rx.recv().await {
                if let Err(err) = attach.input.write_all(&data).await {
                    tracing::error!(error = %err, "failed to write to container stdin");
                }
            }
        });

        let stdout_task = tokio::spawn({
            let stdout_tx = stdout_tx.clone();
            let server_uuid = server.uuid;
            let app_config = Arc::clone(&app_config);

            async move {
                let mut buffer = Vec::with_capacity(1024);
                let mut line_start = 0;

                let mut ratelimit_counter = 0;
                let mut ratelimit_start = std::time::Instant::now();

                let mut allow_ratelimit = || {
                    ratelimit_counter += 1;

                    let config = app_config.load();

                    if config.throttles.enabled
                        && config.throttles.line_reset_interval > 0
                        && ratelimit_counter >= config.throttles.lines
                    {
                        if ratelimit_start.elapsed()
                            < std::time::Duration::from_millis(config.throttles.line_reset_interval)
                        {
                            return false;
                        } else {
                            ratelimit_counter = 0;
                            ratelimit_start = std::time::Instant::now();
                        }
                    }
                    true
                };

                while let Some(Ok(data)) = attach.output.next().await {
                    buffer.extend_from_slice(&data.into_bytes());

                    let mut search_start = line_start;

                    loop {
                        if let Some(pos) = buffer[search_start..].iter().position(|&b| b == b'\n') {
                            let newline_pos = search_start + pos;

                            if newline_pos - line_start <= 512 {
                                let line = compact_str::CompactString::from_utf8_lossy(
                                    &buffer[line_start..newline_pos],
                                )
                                .trim()
                                .into();

                                if allow_ratelimit() {
                                    stdout_tx.send(Arc::new(line)).ok();
                                }

                                line_start = newline_pos + 1;
                                search_start = line_start;
                            } else {
                                let line = compact_str::CompactString::from_utf8_lossy(
                                    &buffer[line_start..(line_start + 512)],
                                )
                                .trim()
                                .into();

                                if allow_ratelimit() {
                                    stdout_tx.send(Arc::new(line)).ok();
                                }

                                line_start += 512;
                                search_start = line_start;
                            }
                        } else {
                            let current_line_length = buffer.len() - line_start;
                            if current_line_length > 512 {
                                let line = compact_str::CompactString::from_utf8_lossy(
                                    &buffer[line_start..(line_start + 512)],
                                )
                                .trim()
                                .into();

                                if allow_ratelimit() {
                                    stdout_tx.send(Arc::new(line)).ok();
                                }

                                line_start += 512;
                                search_start = line_start;
                            } else {
                                break;
                            }
                        }
                    }

                    if line_start > 1024 && line_start > buffer.len() / 2 {
                        buffer.drain(0..line_start);
                        line_start = 0;
                    }
                }

                if line_start < buffer.len() {
                    let line = compact_str::CompactString::from_utf8_lossy(&buffer[line_start..])
                        .trim()
                        .into();

                    if allow_ratelimit() {
                        stdout_tx.send(Arc::new(line)).ok();
                    }
                }

                tracing::debug!(server = %server_uuid, "stdout task ended");
            }
        });

        let stats_docker = Arc::clone(&docker);
        let stats_id = container_id.clone();
        let stats_usage = Arc::clone(&resource_usage);
        let stats_server = server.clone();

        let stats_task = tokio::spawn(async move {
            let mut prev_cpu = (0, 0);

            let mut stream = stats_docker.stats(
                &stats_id,
                Some(bollard::query_parameters::StatsOptions {
                    stream: true,
                    one_shot: false,
                }),
            );

            while let Some(Ok(stats)) = stream.next().await {
                let (disk_bytes, _) = tokio::join!(
                    stats_server.filesystem.limiter_usage(),
                    tokio::time::sleep(std::time::Duration::from_millis(500)),
                );

                let mut usage = stats_usage.write().await;

                if let Some(memory_stats) = &stats.memory_stats {
                    let mut memory_bytes = memory_stats.usage.unwrap_or(0);

                    if let Some(stats) = &memory_stats.stats {
                        if let Some(&inactive_file) = stats.get("total_inactive_file")
                            && inactive_file < memory_bytes
                        {
                            memory_bytes -= inactive_file;
                        } else if let Some(&inactive_file) = stats.get("inactive_file")
                            && inactive_file < memory_bytes
                        {
                            memory_bytes -= inactive_file;
                        }
                    }

                    usage.memory_bytes = memory_bytes;
                    usage.memory_limit_bytes = memory_stats.limit.unwrap_or(0);
                }

                usage.disk_bytes = disk_bytes;
                usage.state = stats_server.state.get_state();

                if let Some(networks) = &stats.networks
                    && let Some(net) = networks.values().next()
                {
                    usage.network.rx_bytes = net.rx_bytes.unwrap_or(0);
                    usage.network.tx_bytes = net.tx_bytes.unwrap_or(0);
                }

                if let Some(cpu_stats) = &stats.cpu_stats
                    && let Some(cpu_usage) = &cpu_stats.cpu_usage
                {
                    usage.cpu_absolute = {
                        let cpu_delta = cpu_usage
                            .total_usage
                            .unwrap_or(0)
                            .saturating_sub(prev_cpu.0)
                            as f64;
                        let sys_delta = cpu_stats
                            .system_cpu_usage
                            .unwrap_or(0)
                            .saturating_sub(prev_cpu.1)
                            as f64;
                        let cpus = cpu_stats.online_cpus.unwrap_or_else(|| {
                            cpu_usage.percpu_usage.as_deref().unwrap_or(&[]).len() as u32
                        }) as f64;

                        if sys_delta > 0.0 && cpu_delta > 0.0 && cpus > 0.0 {
                            ((cpu_delta / sys_delta) * 100.0 * cpus * 1000.0).round() / 1000.0
                        } else {
                            0.0
                        }
                    };

                    prev_cpu = (
                        cpu_usage.total_usage.unwrap_or(0),
                        cpu_stats.system_cpu_usage.unwrap_or(0),
                    );
                }
            }
        });

        let state_docker = Arc::clone(&docker);
        let state_id = container_id.clone();
        let state_usage = Arc::clone(&resource_usage);

        let state_task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                let inspect = state_docker
                    .inspect_container(&state_id, None)
                    .await
                    .unwrap_or_default();
                let state = inspect.state.unwrap_or_default();

                let process_status = match state.status {
                    Some(bollard::plugin::ContainerStateStatusEnum::RUNNING) => {
                        if let Some(ref started_at) = state.started_at
                            && let Ok(started_at) = chrono::DateTime::parse_from_rfc3339(started_at)
                        {
                            let uptime = chrono::Utc::now()
                                .signed_duration_since(started_at.with_timezone(&chrono::Utc))
                                .num_milliseconds()
                                .max(0) as u64;
                            state_usage.write().await.uptime = uptime;
                        }
                        super::ProcessStatus::Running
                    }
                    Some(bollard::plugin::ContainerStateStatusEnum::PAUSED) => {
                        super::ProcessStatus::Paused
                    }
                    _ => {
                        state_usage.write().await.uptime = 0;
                        super::ProcessStatus::Stopped {
                            exit_code: state.exit_code.unwrap_or(-1) as i32,
                            oom_killed: state.oom_killed.unwrap_or(false),
                        }
                    }
                };

                let usage = *state_usage.read().await;

                if status_tx.send((process_status, usage)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Self {
            container_id,
            docker,
            server: Arc::downgrade(&**server),
            app_config,
            resource_usage,
            stdin_tx,
            stdout_rx,
            state_task,
            stats_task,
            stdin_task,
            stdout_task,
        })
    }
}

impl Drop for DockerProcessHandle {
    fn drop(&mut self) {
        self.state_task.abort();
        self.stats_task.abort();
        self.stdin_task.abort();
        self.stdout_task.abort();
    }
}

#[async_trait::async_trait]
impl super::ProcessHandle for DockerProcessHandle {
    async fn resource_usage(
        &self,
    ) -> Result<super::super::resources::ResourceUsage, anyhow::Error> {
        Ok(*self.resource_usage.read().await)
    }

    async fn logs(
        &self,
        lines: Option<usize>,
    ) -> Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>, anyhow::Error> {
        let docker = Arc::clone(&self.docker);
        let container_id = self.container_id.clone();
        let tail = lines.map_or_else(|| "all".to_string(), |n| n.to_string());

        let stream = docker
            .logs(
                &container_id,
                Some(bollard::query_parameters::LogsOptions {
                    follow: false,
                    stdout: true,
                    stderr: true,
                    timestamps: false,
                    tail,
                    ..Default::default()
                }),
            )
            .map(|result| {
                result
                    .map(|log| log.into_bytes().to_vec())
                    .map_err(std::io::Error::other)
            });

        Ok(Box::new(LogsReader {
            stream: Box::pin(stream),
            buffer: Vec::new(),
            pos: 0,
        }))
    }

    async fn send_stdin(&self, data: Vec<u8>) -> Result<(), anyhow::Error> {
        self.stdin_tx.send(data).await.map_err(Into::into)
    }

    async fn subscribe_stdout_lines(
        &self,
    ) -> Result<tokio::sync::broadcast::Receiver<Arc<compact_str::CompactString>>, anyhow::Error>
    {
        Ok(self.stdout_rx.resubscribe())
    }

    async fn sync_configuration(&self) -> Result<(), anyhow::Error> {
        let server = self
            .server
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("server has been dropped"))?;

        let update_config = server
            .configuration
            .read()
            .await
            .container_update_config(&self.app_config);

        self.docker
            .update_container(&self.container_id, update_config)
            .await
            .map_err(Into::into)
    }

    async fn start(&self) -> Result<(), anyhow::Error> {
        self.docker
            .start_container(&self.container_id, None)
            .await
            .map_err(Into::into)
    }

    async fn stop(&self) -> Result<(), anyhow::Error> {
        let server = self
            .server
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("server has been dropped"))?;

        let process_config = server.process_configuration.read().await;
        let stop_type = process_config.stop.r#type.clone();
        let stop_value = process_config.stop.value.clone();
        drop(process_config);

        match stop_type.as_str() {
            "signal" => {
                let signal = match stop_value.as_deref().map(str::to_uppercase).as_deref() {
                    Some("SIGABRT") => "SIGABRT",
                    Some("SIGINT") | Some("C") => "SIGINT",
                    Some("SIGTERM") => "SIGTERM",
                    Some("SIGQUIT") => "SIGQUIT",
                    _ => "SIGKILL",
                };
                self.docker
                    .kill_container(
                        &self.container_id,
                        Some(bollard::query_parameters::KillContainerOptions {
                            signal: signal.to_string(),
                        }),
                    )
                    .await
                    .map_err(Into::into)
            }
            "command" => {
                let mut command = stop_value
                    .map(|s| s.as_bytes().to_vec())
                    .unwrap_or_default();
                command.push(b'\n');
                self.stdin_tx
                    .send(command)
                    .await
                    .map_err(|e| anyhow::anyhow!(e))
            }
            _ => self
                .docker
                .stop_container(
                    &self.container_id,
                    Some(bollard::query_parameters::StopContainerOptions {
                        t: Some(-1),
                        ..Default::default()
                    }),
                )
                .await
                .map_err(Into::into),
        }
    }

    async fn kill(&self) -> Result<(), anyhow::Error> {
        self.docker
            .kill_container(
                &self.container_id,
                Some(bollard::query_parameters::KillContainerOptions {
                    signal: "SIGKILL".to_string(),
                }),
            )
            .await
            .map_err(Into::into)
    }
}

type StatusReceiver =
    tokio::sync::mpsc::Receiver<(super::ProcessStatus, super::super::resources::ResourceUsage)>;

async fn find_running_container(
    docker: &bollard::Docker,
    name_filter: &str,
    exclude_name: Option<&str>,
) -> Option<String> {
    let containers = docker
        .list_containers(Some(bollard::query_parameters::ListContainersOptions {
            all: true,
            filters: Some(HashMap::from([(
                "name".to_string(),
                vec![name_filter.to_string()],
            )])),
            ..Default::default()
        }))
        .await
        .unwrap_or_default();

    for c in containers {
        if let Some(ref excl) = exclude_name
            && c.names
                .as_ref()
                .is_some_and(|names| names.iter().any(|n| n.contains(excl)))
        {
            continue;
        }

        if c.state != Some(bollard::plugin::ContainerSummaryStateEnum::RUNNING) {
            continue;
        }

        if let Some(id) = c.id {
            return Some(id);
        }
    }

    None
}

#[async_trait::async_trait]
impl super::ServerExecutor for DockerExecutor {
    async fn boot(&self) -> Result<(), anyhow::Error> {
        self.app_config.ensure_docker_network(&self.docker).await
    }

    async fn setup_server_process(
        &self,
        server: &super::super::Server,
    ) -> Result<(Arc<dyn super::ProcessHandle>, StatusReceiver), anyhow::Error> {
        let image = server.configuration.read().await.container.image.clone();

        self.pull_image(&image, server, false).await?;

        let container_name = {
            let cfg = server.configuration.read().await;
            if self.app_config.load().docker.server_name_in_container_name {
                let mut filtered = String::new();
                for c in cfg.meta.name.chars() {
                    if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                        filtered.push(c);
                    }
                }
                filtered.truncate(63 - 1 - 36);
                format!("{}.{}", filtered, cfg.uuid)
            } else {
                cfg.uuid.to_string()
            }
        };

        let bollard_config = server
            .configuration
            .read()
            .await
            .container_config(&self.app_config, &self.docker, &server.filesystem)
            .await;

        let container = self
            .docker
            .create_container(
                Some(bollard::query_parameters::CreateContainerOptions {
                    name: Some(container_name),
                    ..Default::default()
                }),
                bollard_config,
            )
            .await?;

        let (status_tx, status_rx) = tokio::sync::mpsc::channel(1);
        let handle = Arc::new(
            DockerProcessHandle::new(
                container.id,
                Arc::clone(&self.docker),
                server,
                Arc::clone(&self.app_config),
                status_tx,
            )
            .await?,
        );

        Ok((handle, status_rx))
    }

    async fn attach_server_process(
        &self,
        server: &super::super::Server,
    ) -> Result<(Arc<dyn super::ProcessHandle>, StatusReceiver), anyhow::Error> {
        let container_id =
            find_running_container(&self.docker, &server.uuid.to_string(), Some("installer"))
                .await
                .ok_or_else(|| anyhow::anyhow!("no running server container found"))?;

        let (status_tx, status_rx) = tokio::sync::mpsc::channel(1);
        let handle = Arc::new(
            DockerProcessHandle::new(
                container_id,
                Arc::clone(&self.docker),
                server,
                Arc::clone(&self.app_config),
                status_tx,
            )
            .await?,
        );

        Ok((handle, status_rx))
    }

    async fn cleanup_server_process(
        &self,
        server: &super::super::Server,
    ) -> Result<(), anyhow::Error> {
        let containers = self
            .docker
            .list_containers(Some(bollard::query_parameters::ListContainersOptions {
                all: true,
                filters: Some(HashMap::from([(
                    "name".to_string(),
                    vec![server.uuid.to_string()],
                )])),
                ..Default::default()
            }))
            .await?;

        for c in containers {
            let Some(id) = c.id else { continue };
            if let Err(err) = self
                .docker
                .remove_container(
                    &id,
                    Some(bollard::query_parameters::RemoveContainerOptions {
                        force: true,
                        ..Default::default()
                    }),
                )
                .await
            {
                tracing::error!(
                    server = %server.uuid,
                    container = %id,
                    "failed to remove container: {}",
                    err
                );
            }
        }

        Ok(())
    }

    async fn setup_installation_process(
        &self,
        server: &super::super::Server,
        script: &super::super::installation::InstallationScript,
    ) -> Result<(Arc<dyn super::ProcessHandle>, StatusReceiver), anyhow::Error> {
        tracing::debug!(
            server = %server.uuid,
            container_image = %script.container_image,
            entrypoint = %script.entrypoint,
            script_len = script.script.len(),
            extra_env_vars = script.environment.len(),
            "setup_installation_process: starting"
        );

        tracing::debug!(
            server = %server.uuid,
            image = %script.container_image,
            "setup_installation_process: pulling installer image"
        );
        if let Err(err) = self.pull_image(&script.container_image, server, false).await {
            tracing::error!(
                server = %server.uuid,
                image = %script.container_image,
                error = %err,
                "setup_installation_process: failed to pull installer image"
            );
            return Err(err);
        }
        tracing::debug!(server = %server.uuid, image = %script.container_image, "setup_installation_process: image ready");

        let server_config = server.configuration.read().await;
        let mut resources = server_config.installer_resources(&self.app_config);
        resources.blkio_weight = None; // blkio_weight is cgroup v1 only; fails on cgroup v2
        tracing::debug!(
            server = %server.uuid,
            memory = ?resources.memory,
            memory_swap = ?resources.memory_swap,
            cpu_quota = ?resources.cpu_quota,
            "setup_installation_process: resolved installer resource limits"
        );

        let mut env = server_config.environment(&self.app_config);
        for (k, v) in &script.environment {
            env.push(format!(
                "{k}={}",
                match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                }
            ));
        }
        tracing::debug!(server = %server.uuid, env_var_count = env.len(), "setup_installation_process: environment built");

        drop(server_config);

        let tmp_dir =
            Path::new(&self.app_config.load().system.tmp_directory).join(server.uuid.to_string());
        tracing::debug!(server = %server.uuid, tmp_dir = %tmp_dir.display(), "setup_installation_process: creating tmp directory");
        if let Err(err) = tokio::fs::create_dir_all(&tmp_dir).await {
            tracing::error!(
                server = %server.uuid,
                tmp_dir = %tmp_dir.display(),
                error = %err,
                "setup_installation_process: failed to create tmp directory"
            );
            return Err(err.into());
        }

        let install_script_path = tmp_dir.join("install.sh");
        tracing::debug!(server = %server.uuid, path = %install_script_path.display(), "setup_installation_process: writing install.sh");
        if let Err(err) = tokio::fs::write(&install_script_path, script.script.replace("\r\n", "\n")).await {
            tracing::error!(
                server = %server.uuid,
                path = %install_script_path.display(),
                error = %err,
                "setup_installation_process: failed to write install.sh"
            );
            return Err(err.into());
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tracing::debug!(server = %server.uuid, tmp_dir = %tmp_dir.display(), "setup_installation_process: setting tmp dir permissions to 0o755");
            if let Err(err) = tokio::fs::set_permissions(&tmp_dir, std::fs::Permissions::from_mode(0o755)).await {
                tracing::error!(
                    server = %server.uuid,
                    tmp_dir = %tmp_dir.display(),
                    error = %err,
                    "setup_installation_process: failed to set tmp dir permissions"
                );
                return Err(err.into());
            }
        }

        let container_name = format!("{}_installer", server.uuid);
        let server_data_dir = server.filesystem.base().to_string();
        let network_mode = self.app_config.load().docker.network.mode.clone();
        tracing::debug!(
            server = %server.uuid,
            container_name = %container_name,
            image = %script.container_image,
            entrypoint = %script.entrypoint,
            server_data_dir = %server_data_dir,
            tmp_dir = %tmp_dir.display(),
            network_mode = %network_mode,
            "setup_installation_process: creating installer container"
        );

        let bollard_config = bollard::plugin::ContainerCreateBody {
            host_config: Some(bollard::plugin::HostConfig {
                memory: resources.memory,
                memory_reservation: resources.memory_reservation,
                memory_swap: resources.memory_swap,
                cpu_quota: resources.cpu_quota,
                cpu_period: resources.cpu_period,
                cpu_shares: resources.cpu_shares,
                cpuset_cpus: resources.cpuset_cpus,
                pids_limit: resources.pids_limit,
                blkio_weight: resources.blkio_weight,
                oom_kill_disable: resources.oom_kill_disable,
                mounts: Some(vec![
                    bollard::plugin::Mount {
                        typ: Some(bollard::plugin::MountType::BIND),
                        source: Some(server_data_dir.clone()),
                        target: Some("/mnt/server".to_string()),
                        ..Default::default()
                    },
                    bollard::plugin::Mount {
                        typ: Some(bollard::plugin::MountType::BIND),
                        source: Some(tmp_dir.to_string_lossy().into_owned()),
                        target: Some("/mnt/install".to_string()),
                        ..Default::default()
                    },
                ]),
                network_mode: Some(network_mode),
                dns: Some(self.app_config.load().docker.network.dns.clone()),
                tmpfs: Some(HashMap::from([(
                    "/tmp".to_string(),
                    format!(
                        "rw,exec,nosuid,size={}M",
                        self.app_config.load().docker.tmpfs_size
                    ),
                )])),
                log_config: Some(bollard::plugin::HostConfigLogConfig {
                    typ: Some(self.app_config.load().docker.log_config.r#type.clone()),
                    config: Some(
                        self.app_config
                            .load()
                            .docker
                            .log_config
                            .config
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect(),
                    ),
                }),
                userns_mode: string_to_option(&self.app_config.load().docker.userns_mode),
                ..Default::default()
            }),
            entrypoint: Some({
                let shell = script.entrypoint.split_whitespace().next().unwrap_or("bash");
                vec![shell.to_string()]
            }),
            cmd: Some(vec!["/mnt/install/install.sh".to_string()]),
            hostname: Some("installer".to_string()),
            image: Some(script.container_image.trim_end_matches('~').to_string()),
            env: Some(env),
            labels: Some(HashMap::from([
                (
                    "Service".to_string(),
                    self.app_config.load().app_name.clone(),
                ),
                ("ContainerType".to_string(), "server_installer".to_string()),
            ])),
            attach_stdin: Some(true),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            open_stdin: Some(true),
            tty: Some(true),
            ..Default::default()
        };

        let container = match self
            .docker
            .create_container(
                Some(bollard::query_parameters::CreateContainerOptions {
                    name: Some(container_name.clone()),
                    ..Default::default()
                }),
                bollard_config,
            )
            .await
        {
            Ok(c) => {
                tracing::debug!(
                    server = %server.uuid,
                    container_id = %c.id,
                    container_name = %container_name,
                    "setup_installation_process: container created"
                );
                c
            }
            Err(err) => {
                tracing::error!(
                    server = %server.uuid,
                    container_name = %container_name,
                    error = %err,
                    "setup_installation_process: failed to create installer container"
                );
                return Err(err.into());
            }
        };

        let (status_tx, status_rx) = tokio::sync::mpsc::channel(1);

        tracing::debug!(
            server = %server.uuid,
            container_id = %container.id,
            "setup_installation_process: attaching to installer container"
        );
        let handle = match DockerProcessHandle::new(
            container.id.clone(),
            Arc::clone(&self.docker),
            server,
            Arc::clone(&self.app_config),
            status_tx,
        )
        .await
        {
            Ok(h) => {
                tracing::debug!(
                    server = %server.uuid,
                    container_id = %container.id,
                    "setup_installation_process: attached to installer container successfully"
                );
                Arc::new(h)
            }
            Err(err) => {
                tracing::error!(
                    server = %server.uuid,
                    container_id = %container.id,
                    error = %err,
                    "setup_installation_process: failed to attach to installer container"
                );
                return Err(err);
            }
        };

        Ok((handle, status_rx))
    }

    async fn attach_installation_process(
        &self,
        server: &super::super::Server,
    ) -> Result<(Arc<dyn super::ProcessHandle>, StatusReceiver), anyhow::Error> {
        let container_id =
            find_running_container(&self.docker, &format!("{}_installer", server.uuid), None)
                .await
                .ok_or_else(|| anyhow::anyhow!("no running installer container found"))?;

        let (status_tx, status_rx) = tokio::sync::mpsc::channel(1);
        let handle = Arc::new(
            DockerProcessHandle::new(
                container_id,
                Arc::clone(&self.docker),
                server,
                Arc::clone(&self.app_config),
                status_tx,
            )
            .await?,
        );

        Ok((handle, status_rx))
    }

    async fn cleanup_installation_process(
        &self,
        server: &super::super::Server,
    ) -> Result<(), anyhow::Error> {
        let containers = self
            .docker
            .list_containers(Some(bollard::query_parameters::ListContainersOptions {
                all: true,
                filters: Some(HashMap::from([(
                    "name".to_string(),
                    vec![format!("{}_installer", server.uuid)],
                )])),
                ..Default::default()
            }))
            .await?;

        for c in containers {
            let Some(id) = c.id else { continue };
            if let Err(err) = self
                .docker
                .remove_container(
                    &id,
                    Some(bollard::query_parameters::RemoveContainerOptions {
                        force: true,
                        ..Default::default()
                    }),
                )
                .await
            {
                tracing::error!(
                    server = %server.uuid,
                    container = %id,
                    "failed to remove installation container: {}",
                    err
                );
            }
        }

        Ok(())
    }

    async fn setup_script_process(
        &self,
        server: &super::super::Server,
        script: &super::super::installation::InstallationScript,
    ) -> Result<(Arc<dyn super::ProcessHandle>, StatusReceiver), anyhow::Error> {
        self.pull_image(&script.container_image, server, true)
            .await?;

        let server_config = server.configuration.read().await;
        let mut resources = server_config.installer_resources(&self.app_config);
        resources.blkio_weight = None; // blkio_weight is cgroup v1 only; fails on cgroup v2

        let mut env = server_config.environment(&self.app_config);
        for (k, v) in &script.environment {
            env.push(format!(
                "{k}={}",
                match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                }
            ));
        }

        drop(server_config);

        let tmp_dir =
            Path::new(&self.app_config.load().system.tmp_directory).join(server.uuid.to_string());
        tokio::fs::create_dir_all(&tmp_dir).await?;
        tokio::fs::write(
            tmp_dir.join("script.sh"),
            script.script.replace("\r\n", "\n"),
        )
        .await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&tmp_dir, std::fs::Permissions::from_mode(0o755)).await?;
        }

        let bollard_config = bollard::plugin::ContainerCreateBody {
            host_config: Some(bollard::plugin::HostConfig {
                memory: resources.memory,
                memory_reservation: resources.memory_reservation,
                memory_swap: resources.memory_swap,
                cpu_quota: resources.cpu_quota,
                cpu_period: resources.cpu_period,
                cpu_shares: resources.cpu_shares,
                cpuset_cpus: resources.cpuset_cpus,
                pids_limit: resources.pids_limit,
                blkio_weight: resources.blkio_weight,
                oom_kill_disable: resources.oom_kill_disable,
                mounts: Some(vec![
                    bollard::plugin::Mount {
                        typ: Some(bollard::plugin::MountType::BIND),
                        source: Some(server.filesystem.base().into()),
                        target: Some("/mnt/server".to_string()),
                        ..Default::default()
                    },
                    bollard::plugin::Mount {
                        typ: Some(bollard::plugin::MountType::BIND),
                        source: Some(tmp_dir.to_string_lossy().into_owned()),
                        target: Some("/mnt/script".to_string()),
                        ..Default::default()
                    },
                ]),
                network_mode: Some(self.app_config.load().docker.network.mode.clone()),
                dns: Some(self.app_config.load().docker.network.dns.clone()),
                tmpfs: Some(HashMap::from([(
                    "/tmp".to_string(),
                    format!(
                        "rw,exec,nosuid,size={}M",
                        self.app_config.load().docker.tmpfs_size
                    ),
                )])),
                log_config: Some(bollard::plugin::HostConfigLogConfig {
                    typ: Some(self.app_config.load().docker.log_config.r#type.clone()),
                    config: Some(
                        self.app_config
                            .load()
                            .docker
                            .log_config
                            .config
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect(),
                    ),
                }),
                userns_mode: string_to_option(&self.app_config.load().docker.userns_mode),
                auto_remove: Some(true),
                ..Default::default()
            }),
            entrypoint: Some({
                let shell = script.entrypoint.split_whitespace().next().unwrap_or("bash");
                vec![shell.to_string()]
            }),
            cmd: Some(vec!["/mnt/script/script.sh".to_string()]),
            hostname: Some("script".to_string()),
            image: Some(script.container_image.trim_end_matches('~').to_string()),
            env: Some(env),
            labels: Some(HashMap::from([
                (
                    "Service".to_string(),
                    self.app_config.load().app_name.clone(),
                ),
                ("ContainerType".to_string(), "script_runner".to_string()),
            ])),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            tty: Some(false),
            ..Default::default()
        };

        let name = format!(
            "{}_script_runner_{}",
            server.uuid,
            rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 8)
        );

        let container = self
            .docker
            .create_container(
                Some(bollard::query_parameters::CreateContainerOptions {
                    name: Some(name),
                    ..Default::default()
                }),
                bollard_config,
            )
            .await?;

        let (status_tx, status_rx) = tokio::sync::mpsc::channel(1);
        let handle = Arc::new(
            DockerProcessHandle::new(
                container.id,
                Arc::clone(&self.docker),
                server,
                Arc::clone(&self.app_config),
                status_tx,
            )
            .await?,
        );

        Ok((handle, status_rx))
    }
}
