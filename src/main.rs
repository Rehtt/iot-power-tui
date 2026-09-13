mod archive;
mod client;
mod domain;
mod history;
mod network;
mod protocol;
mod recording;
mod runtime;
mod source;
mod storage;
mod ui;
mod workspace;

use anyhow::{ensure, Context, Result};
use clap::{ArgGroup, Parser};
use crossterm::{
    event::{self, Event, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};
use runtime::{Runtime, State};
use std::{
    io::stdout,
    time::{Duration, Instant},
};

#[derive(Parser, Debug)]
#[command(name="iot-power-tui",about="IoT Power CC USB 终端采集工具",group(ArgGroup::new("source").args(["usb","mock","replay","port","list_devices"]).required(false)))]
#[command(group(ArgGroup::new("input_or_mode").args(["usb", "mock", "replay", "port", "list_devices", "service", "client", "history"]).required(true).multiple(true)))]
#[command(group(ArgGroup::new("remote").args(["service", "client"])))]
#[command(group(ArgGroup::new("usb_mode").args(["usb", "service"]).multiple(true)))]
struct Args {
    #[arg(long)]
    usb: bool,
    #[arg(long, conflicts_with_all=["client","list_devices"])]
    service: bool,
    #[arg(long, conflicts_with_all=["usb","mock","replay","port","device","list_devices","db","baud"])]
    client: bool,
    #[arg(long, conflicts_with_all=["usb","mock","replay","port","service","client","list_devices","device"])]
    history: bool,
    #[arg(long, requires = "remote")]
    addr: Option<std::net::SocketAddr>,
    #[arg(long, requires = "client", conflicts_with_all=["source","service","device"], default_value = "./downloads")]
    download_dir: String,
    #[arg(long,requires="usb_mode",conflicts_with_all=["mock","replay","port","list_devices"])]
    device: Option<String>,
    #[arg(long)]
    list_devices: bool,
    /// Serial JSONL input, not the native CC USB protocol.
    #[arg(long)]
    port: Option<String>,
    #[arg(long, default_value_t = 115200)]
    baud: u32,
    #[arg(long)]
    mock: bool,
    #[arg(long)]
    replay: Option<String>,
    #[arg(long, default_value = "./data/iot-power.db")]
    db: String,
    /// Bytes per recording buffer (two buffers); M is decimal, MiB is binary.
    #[arg(long, default_value="10M",value_parser=recording::parse_size,conflicts_with_all=["client","list_devices"])]
    buffer_size: usize,
    /// Software recording rate in Hz; CC acquisition remains 10000 Hz.
    #[arg(long,default_value_t=10000,conflicts_with_all=["client","list_devices"])]
    sample_rate: u32,
}
impl Args {
    fn config(&self) -> recording::Config {
        recording::Config {
            buffer_size_bytes: self.buffer_size,
            sample_rate_hz: self.sample_rate,
        }
    }

    fn source(&self) -> Box<dyn source::DataSource> {
        if self.usb || (self.service && !self.mock && self.replay.is_none() && self.port.is_none())
        {
            Box::new(source::usb::UsbSource {
                serial: self.device.clone(),
            })
        } else if self.mock {
            Box::new(source::mock::MockSource)
        } else if let Some(path) = &self.replay {
            Box::new(source::replay::ReplaySource { path: path.clone() })
        } else {
            Box::new(source::serial::SerialSource {
                port: self.port.clone().expect("validated CLI source"),
                baud: self.baud,
            })
        }
    }
}
struct TerminalGuard;
impl TerminalGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode()?;
        let guard = Self;
        execute!(stdout(), EnterAlternateScreen)?;
        Ok(guard)
    }
}
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(stdout(), LeaveAlternateScreen, crossterm::cursor::Show);
    }
}
#[derive(Clone)]
enum OperationKind {
    Stop,
    Reconfigure(recording::Config),
    CheckEmpty,
    Finish(ui::Finish),
    Rotate { save: bool, name: String },
}
struct Completion {
    exit: bool,
    saved: bool,
    warning: Option<String>,
}
struct Operation {
    handle:
        Option<std::thread::JoinHandle<(Runtime, workspace::CaptureWorkspace, Result<Completion>)>>,
    kind: OperationKind,
}
impl Drop for Operation {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
fn begin_operation(
    mut runtime: Runtime,
    mut workspace: workspace::CaptureWorkspace,
    kind: OperationKind,
    progress: std::sync::Arc<std::sync::Mutex<ui::Progress>>,
) -> Operation {
    let thread_kind = kind.clone();
    let handle = std::thread::spawn(move || {
        *progress.lock().unwrap() = ui::Progress {
            message: "停止采集并暂存剩余数据".into(),
            ..Default::default()
        };
        runtime.stop();
        let result = (|| -> Result<Completion> {
            match thread_kind {
                OperationKind::Rotate { save, name } => {
                    if save {
                        runtime.ensure_saved()?;
                        let conn = rusqlite::Connection::open(&workspace.database)?;
                        let id: i64 = conn.query_row(
                            "SELECT id FROM sessions ORDER BY id DESC LIMIT 1",
                            [],
                            |r| r.get(0),
                        )?;
                        crate::storage::update_session_name(&workspace.database, id, &name)?;
                        progress.lock().unwrap().message = "保存旧会话到目标数据库".into();
                        workspace.save(|done, total| {
                            let mut p = progress.lock().unwrap();
                            p.done = done;
                            p.total = total;
                        })?;
                    } else {
                        progress.lock().unwrap().message = "丢弃旧会话数据".into();
                        workspace.discard()?;
                    }
                    Ok(Completion {
                        exit: false,
                        saved: save,
                        warning: None,
                    })
                }
                OperationKind::Reconfigure(_) => {
                    runtime.ensure_saved()?;
                    Ok(Completion {
                        exit: false,
                        saved: false,
                        warning: None,
                    })
                }
                OperationKind::Stop => Ok(Completion {
                    exit: false,
                    saved: false,
                    warning: None,
                }),
                OperationKind::CheckEmpty => {
                    let empty = runtime.shared.lock().unwrap().accepted == 0;
                    if empty {
                        workspace.discard()?;
                    }
                    Ok(Completion {
                        exit: empty,
                        saved: false,
                        warning: None,
                    })
                }
                OperationKind::Finish(ui::Finish::Save) => {
                    runtime.ensure_saved()?;
                    progress.lock().unwrap().message = "保存到目标数据库".into();
                    let warning = workspace.save(|done, total| {
                        let mut p = progress.lock().unwrap();
                        p.done = done;
                        p.total = total;
                    })?;
                    Ok(Completion {
                        exit: true,
                        saved: true,
                        warning,
                    })
                }
                OperationKind::Finish(ui::Finish::Discard) => {
                    progress.lock().unwrap().message = "清理本次临时数据".into();
                    workspace.discard()?;
                    Ok(Completion {
                        exit: true,
                        saved: false,
                        warning: None,
                    })
                }
            }
        })();
        (runtime, workspace, result)
    });
    Operation {
        handle: Some(handle),
        kind,
    }
}
fn main() -> Result<()> {
    let args = Args::parse();
    args.config().validate()?;
    ensure!(
        args.addr.is_none() || args.service || args.client,
        "--addr requires --service or --client"
    );
    ensure!(
        args.device.is_none() || args.usb || args.service,
        "--device requires USB"
    );
    if args.list_devices {
        return source::usb::list_devices();
    }
    if args.history {
        return show_history(&args.db);
    }
    if args.service {
        return network::run(&args);
    }
    if args.client {
        return client::run(&args);
    }
    ensure!(
        args.usb || args.mock || args.replay.is_some() || args.port.is_some(),
        "choose --usb, --mock, --replay, --port, --service or --client"
    );
    let workspace = workspace::CaptureWorkspace::new(&args.db)?;
    let cache = workspace.database.display().to_string();
    run_tui(&args, workspace).with_context(|| format!("未清理的采集缓存（若存在）：{cache}"))
}
fn show_history(path: &str) -> Result<()> {
    archive::browse(vec![std::path::PathBuf::from(path)])
}
fn run_tui(args: &Args, workspace: workspace::CaptureWorkspace) -> Result<()> {
    let target_label = workspace.target.display().to_string();
    let mut cache_label = workspace.database.display().to_string();
    let guard = TerminalGuard::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;
    let mut config = args.config();
    let mut editor: Option<ui::ConfigEditor> = None;
    let mut runtime = Some(Runtime::start_config(
        args.source(),
        cache_label.clone(),
        config,
    ));
    let mut shared = runtime.as_ref().unwrap().shared.clone();
    let mut workspace = Some(workspace);
    let mut totals = ui::Totals::default();
    let mut settings = ui::UiState::default();
    let progress = std::sync::Arc::new(std::sync::Mutex::new(ui::Progress::default()));
    let mut operation: Option<Operation> = None;
    let mut notice: Option<String> = None;
    let mut session_prompt: Option<ui::SessionPrompt> = None;
    let mut rate_at = Instant::now();
    let mut rate_count = 0;
    let mut rate = 0.0;
    let completion = loop {
        if operation
            .as_ref()
            .is_some_and(|job| job.handle.as_ref().unwrap().is_finished())
        {
            let mut job = operation.take().unwrap();
            let kind = job.kind.clone();
            let (returned_runtime, returned_workspace, result) = job
                .handle
                .take()
                .unwrap()
                .join()
                .map_err(|_| anyhow::anyhow!("background operation panicked"))?;
            runtime = Some(returned_runtime);
            workspace = Some(returned_workspace);
            match result {
                Ok(completion) if completion.exit => break completion,
                Ok(_) => {
                    if let OperationKind::Reconfigure(next) = kind {
                        totals.add(&shared.lock().unwrap());
                        config = next;
                        runtime = Some(Runtime::start_config(
                            args.source(),
                            cache_label.clone(),
                            config,
                        ));
                        shared = runtime.as_ref().unwrap().shared.clone();
                        rate_at = Instant::now();
                        rate_count = 0;
                        rate = 0.0;
                        notice = None;
                    }
                    if matches!(&kind, OperationKind::Stop) {
                        session_prompt = Some(ui::SessionPrompt::new(
                            workspace.as_ref().unwrap().suggested_name()?,
                        ));
                    }
                    if matches!(kind, OperationKind::Rotate { .. }) {
                        let next_workspace = workspace::CaptureWorkspace::new(&target_label)?;
                        cache_label = next_workspace.database.display().to_string();
                        workspace = Some(next_workspace);
                        rate_at = Instant::now();
                        rate_count = 0;
                        rate = 0.0;
                        runtime = Some(Runtime::start_config(
                            args.source(),
                            workspace.as_ref().unwrap().database.display().to_string(),
                            config,
                        ));
                        shared = runtime.as_ref().unwrap().shared.clone();
                    }
                    if matches!(kind, OperationKind::CheckEmpty) {
                        settings.dialog = Some(0);
                    }
                }
                Err(e) => {
                    notice = Some(format!("操作失败：{e:#}。可重试保存或选择不保存。"));
                    if let OperationKind::Reconfigure(next) = kind {
                        editor = Some(ui::ConfigEditor::new(next));
                        editor.as_mut().unwrap().error = notice.clone();
                    } else if let OperationKind::Rotate { name, .. } = kind {
                        session_prompt = Some(ui::SessionPrompt::new(name));
                    } else {
                        settings.dialog = Some(0);
                    }
                }
            }
        }
        let s = shared.lock().unwrap().clone();
        let mut current = totals.clone();
        current.add(&s);
        if rate_at.elapsed() >= Duration::from_secs(1) {
            rate = s.received.saturating_sub(rate_count) as f64 / rate_at.elapsed().as_secs_f64();
            rate_count = s.received;
            rate_at = Instant::now();
        }
        let snapshot = progress.lock().unwrap().clone();
        let frame_deadline = Instant::now() + Duration::from_millis(100);
        terminal.draw(|f| {
            ui::render(
                f,
                ui::View {
                    state: &s,
                    totals: &current,
                    settings: &settings,
                    rate,
                    target: &target_label,
                    remote: false,
                    cache: &cache_label,
                    progress: operation.as_ref().map(|_| &snapshot),
                    notice: notice.as_deref(),
                },
            );
            if let Some(edit) = &editor {
                edit.render(f);
            }
            if let Some(prompt) = &session_prompt {
                prompt.render(f);
            }
        })?;
        if !event::poll(frame_deadline.saturating_duration_since(Instant::now()))? {
            continue;
        }
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind != KeyEventKind::Press || operation.is_some() {
            continue;
        }
        if key.code == event::KeyCode::Char('h')
            && session_prompt.is_none()
            && editor.is_none()
            && settings.dialog.is_none()
        {
            archive::browse(vec![
                std::path::PathBuf::from(&target_label),
                std::path::PathBuf::from(&cache_label),
            ])?;
            enable_raw_mode()?;
            execute!(stdout(), EnterAlternateScreen)?;
            terminal.clear()?;
            continue;
        }
        if let Some(prompt) = &mut session_prompt {
            if let Some(choice) = prompt.key(key) {
                let name = prompt.name.clone();
                session_prompt = None;
                operation = Some(begin_operation(
                    runtime.take().unwrap(),
                    workspace.take().unwrap(),
                    OperationKind::Rotate {
                        save: choice == ui::SessionChoice::Save,
                        name,
                    },
                    progress.clone(),
                ));
            }
            continue;
        }
        let edit_result = editor.as_mut().and_then(|edit| edit.key(key));
        if editor.as_ref().is_some_and(|edit| edit.closed) {
            editor = None;
            continue;
        }
        if editor.is_some() && edit_result.is_none() {
            continue;
        }
        let kind = if let Some(next) = edit_result {
            editor = None;
            if next == config && s.state == State::Capturing {
                None
            } else {
                Some(OperationKind::Reconfigure(next))
            }
        } else {
            match settings.key(key) {
                ui::Action::Configure => {
                    editor = Some(ui::ConfigEditor::new(config));
                    None
                }
                ui::Action::None => None,
                ui::Action::Reset => {
                    runtime.as_ref().unwrap().reset_metrics();
                    None
                }
                ui::Action::RequestExit => {
                    if current.accepted == 0 {
                        Some(OperationKind::CheckEmpty)
                    } else {
                        settings.dialog = Some(0);
                        None
                    }
                }
                ui::Action::Finish(action) => Some(OperationKind::Finish(action)),
                ui::Action::StopRestart => Some(OperationKind::Stop),
            }
        };
        if let Some(kind) = kind {
            settings.dialog = None;
            progress.lock().unwrap().message = "准备处理…".into();
            operation = Some(begin_operation(
                runtime.take().unwrap(),
                workspace.take().unwrap(),
                kind,
                progress.clone(),
            ));
        }
    };
    totals.add(&shared.lock().unwrap());
    drop(terminal);
    drop(guard);
    println!("Capture: sessions={} received={} accepted={} staged={} saved={} missing_packets={} invalid={} dropped={} destination={}",totals.sessions,totals.received,totals.accepted,totals.staged,if completion.saved {totals.staged}else{0},totals.gaps,totals.invalid,totals.dropped,if completion.saved {target_label.as_str()}else{"discarded"});
    if let Some(warning) = completion.warning {
        eprintln!("{warning}");
    }
    ensure!(
        totals.error.is_none(),
        "{}",
        totals.error.unwrap_or_default()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recording_cli_defaults_units_and_invalid_rates() {
        let default = Args::try_parse_from(["app", "--usb"]).unwrap();
        assert_eq!(default.config(), recording::Config::default());
        let changed = Args::try_parse_from([
            "app",
            "--service",
            "--buffer-size",
            "10MiB",
            "--sample-rate",
            "333",
        ])
        .unwrap();
        assert_eq!(
            changed.config().validate().unwrap(),
            recording::Config {
                buffer_size_bytes: 10_485_760,
                sample_rate_hz: 333
            }
        );
        for rate in ["0", "10001"] {
            assert!(
                Args::try_parse_from(["app", "--usb", "--sample-rate", rate])
                    .unwrap()
                    .config()
                    .validate()
                    .is_err()
            );
        }
        assert!(Args::try_parse_from(["app", "--client", "--sample-rate", "333"]).is_err());
    }
    #[test]
    fn service_client_cli_modes_and_conflicts() {
        for arguments in [
            vec!["app", "--service"],
            vec!["app", "--service", "--device", "CC"],
            vec!["app", "--service", "--mock"],
            vec!["app", "--client", "--addr", "127.0.0.1:8080"],
            vec!["app", "--client", "--download-dir", "downloads"],
        ] {
            assert!(Args::try_parse_from(arguments).is_ok());
        }
        for arguments in [
            vec!["app", "--service", "--client"],
            vec!["app", "--client", "--db", "capture.db"],
            vec!["app", "--client", "--mock"],
            vec!["app", "--mock", "--download-dir", "downloads"],
            vec!["app", "--service", "--list-devices"],
        ] {
            assert!(Args::try_parse_from(&arguments).is_err(), "{arguments:?}");
        }
    }
    #[test]
    fn requires_one_explicit_source_and_usb_for_device_selector() {
        assert!(Args::try_parse_from(["app", "--service"]).is_ok());
        assert!(Args::try_parse_from(["app", "--usb", "--mock"]).is_err());
        assert!(Args::try_parse_from(["app", "--mock", "--device", "CC"]).is_err());
        assert!(Args::try_parse_from(["app", "--usb", "--device", "CC"]).is_ok());
        assert!(Args::try_parse_from(["app", "--list-devices"]).is_ok());
    }
}
