//! Network workers never hold the terminal or write to the service database.
use crate::{
    network::{Live, Session},
    ui, Args, TerminalGuard,
};
use anyhow::{ensure, Context, Result};
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::{
    backend::CrosstermBackend,
    widgets::{Block, Borders, Clear, Paragraph},
    Terminal,
};
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};
#[derive(Default)]
struct Remote {
    live: Option<Live>,
    seen: Option<Instant>,
    error: Option<String>,
    sessions: Vec<Session>,
    message: String,
    downloading: bool,
}
#[derive(Clone)]
enum Command {
    Sessions(Option<i64>),
    Download(i64),
    Refresh,
}
struct Workers {
    stop: Arc<AtomicBool>,
    handles: Vec<thread::JoinHandle<()>>,
}
impl Drop for Workers {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
    }
}
async fn download(
    http: &reqwest::Client,
    base: &str,
    id: i64,
    dir: &Path,
    stop: &AtomicBool,
) -> Result<PathBuf> {
    std::fs::create_dir_all(dir).context("create download directory")?;
    let mut response = http
        .get(format!("{base}/api/v1/sessions/{id}/download"))
        .send()
        .await?
        .error_for_status()?;
    let mut file = tempfile::Builder::new()
        .prefix(&format!(
            "session-{id}-{}-",
            chrono::Utc::now().format("%Y%m%dT%H%M%S")
        ))
        .suffix(".part")
        .tempfile_in(dir)?;
    while let Some(chunk) = response.chunk().await? {
        ensure!(!stop.load(Ordering::Relaxed), "download cancelled");
        file.write_all(&chunk)?;
    }
    file.flush()?;
    file.as_file().sync_all()?;
    {
        let conn = crate::network::read_database(file.path())?;
        let check: String = conn.query_row("PRAGMA quick_check", [], |r| r.get(0))?;
        ensure!(check == "ok", "invalid SQLite download");
        ensure!(
            conn.prepare("PRAGMA foreign_key_check")?
                .query([])?
                .next()?
                .is_none(),
            "invalid foreign keys"
        );
        let (session, count): (i64, u64) = conn.query_row(
            "SELECT source_session_id,(SELECT count(*) FROM sessions) FROM export_metadata",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(session == id && count == 1, "unexpected exported session");
    }
    ensure!(!stop.load(Ordering::Relaxed), "download cancelled");
    let target = file.path().with_extension("db");
    file.persist_noclobber(&target)?;
    Ok(target)
}
pub fn run(args: &Args) -> Result<()> {
    let addr = args
        .addr
        .unwrap_or_else(|| "127.0.0.1:8080".parse().unwrap());
    let base = format!("http://{addr}");
    let shared = Arc::new(Mutex::new(Remote::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let refresh = Arc::new(AtomicBool::new(false));
    let refresh_requested = refresh.clone();
    let http = reqwest::blocking::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(3))
        .build()?;
    let (tx, rx) = std::sync::mpsc::sync_channel(4);
    let mut workers = Workers {
        stop: stop.clone(),
        handles: vec![],
    };
    let (data, cancel, url, agent) = (shared.clone(), stop.clone(), base.clone(), http.clone());
    workers.handles.push(thread::spawn(move || {
        while !cancel.load(Ordering::Relaxed) {
            let result = (|| -> Result<Live> {
                let response = agent
                    .get(format!("{url}/api/v1/status"))
                    .send()?
                    .error_for_status()?;
                let mut bytes = Vec::new();
                response.take(512 * 1024 + 1).read_to_end(&mut bytes)?;
                ensure!(bytes.len() <= 512 * 1024, "status exceeds limit");
                let live: Live = serde_json::from_slice(&bytes)?;
                ensure!(
                    live.version == 1 && live.buckets.len() <= 600,
                    "unsupported live response"
                );
                Ok(live)
            })();
            {
                let mut remote = data.lock().unwrap();
                match result {
                    Ok(live) => {
                        remote.live = Some(live);
                        remote.seen = Some(Instant::now());
                        remote.error = None;
                    }
                    Err(e) => remote.error = Some(format!("离线：{e}")),
                }
            }
            for _ in 0..10 {
                if cancel.load(Ordering::Relaxed)
                    || refresh_requested.swap(false, Ordering::Relaxed)
                {
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
        }
    }));
    let (data, cancel, url, dir) = (
        shared.clone(),
        stop.clone(),
        base.clone(),
        PathBuf::from(&args.download_dir),
    );
    workers.handles.push(thread::spawn(move || {
        let executor = tokio::runtime::Runtime::new().expect("download runtime");
        let download_http = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(300))
            .build()
            .expect("HTTP client");
        while !cancel.load(Ordering::Relaxed) {
            let command = match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(c) => c,
                Err(_) => continue,
            };
            let downloading = matches!(command,Command::Download(_));
            let result = (|| -> Result<String> {
                match command {
                    Command::Sessions(before) => {
                        let mut request = http.get(format!("{url}/api/v1/sessions"));
                        if let Some(id) = before {
                            request = request.query(&[("before", id)]);
                        }
                        let mut bytes = Vec::new();
                        request.send()?.error_for_status()?.take(256 * 1024 + 1).read_to_end(&mut bytes)?;
                        ensure!(bytes.len() <= 256 * 1024, "oversized session response");
                        let sessions: Vec<Session> = serde_json::from_slice(&bytes)?;
                        ensure!(sessions.len() <= 50, "oversized session page");
                        data.lock().unwrap().sessions = sessions;
                        Ok("↑/↓ 选择；PgDn 下一页；r 返回最新；d 下载；Esc 关闭".into())
                    }
                    Command::Download(id) => {
                        data.lock().unwrap().message = format!("正在下载会话 {id}…");
                        let path = executor.block_on(async {
                            tokio::select! {
                                result = download(&download_http, &url, id, &dir, &cancel) => result,
                                _ = async {
                                    while !cancel.load(Ordering::Relaxed) {
                                        tokio::time::sleep(Duration::from_millis(50)).await;
                                    }
                                } => anyhow::bail!("download cancelled")
                            }
                        })?;
                        Ok(format!("已下载：{}", path.display()))
                    }
                    Command::Refresh => Ok("实时数据每 200 ms 自动刷新".into()),
                }
            })();
            let mut remote = data.lock().unwrap();
            if downloading { remote.downloading = false; }
            remote.message = match result {
                Ok(s) => s,
                Err(e) => format!("操作失败：{e:#}"),
            };
        }
    }));
    let _guard = TerminalGuard::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(std::io::stdout()))?;
    let mut settings = ui::UiState::default();
    let mut history = false;
    let mut selection = 0usize;
    loop {
        {
            let data = shared.lock().unwrap();
            let state = data.live.as_ref().map(Live::shared).unwrap_or_default();
            let mut totals = ui::Totals::default();
            totals.add(&state);
            let fresh = data
                .seen
                .map(|t| format!("{:.1}s", t.elapsed().as_secs_f64()))
                .unwrap_or_else(|| "等待连接".into());
            let notice = format!(
                "{addr}  {}  更新距今 {fresh}  {}",
                data.error.as_deref().unwrap_or(
                    if data
                        .seen
                        .is_some_and(|t| t.elapsed() < Duration::from_secs(3))
                    {
                        "在线"
                    } else {
                        "等待连接 / 数据过期"
                    }
                ),
                format_args!("{} {}", data.message, state.error.as_deref().unwrap_or(""))
            );
            terminal.draw(|f| {
                ui::render(
                    f,
                    ui::View {
                        state: &state,
                        totals: &totals,
                        settings: &settings,
                        rate: data.live.as_ref().map_or(0.0, |l| l.rate),
                        target: &base,
                        remote: true,
                        cache: "",
                        progress: None,
                        notice: Some(&notice),
                    },
                );
                if history {
                    let area = f.area();
                    f.render_widget(Clear, area);
                    let mut lines = vec![
                        "服务端会话 · ↑/↓ 选择 d 下载 PgDn 下一页 r 最新 Esc 关闭".to_string(),
                        data.message.clone(),
                    ];
                    let visible = area.height.saturating_sub(4).max(1) as usize;
                    let offset = selection.saturating_sub(visible - 1);
                    for (i, s) in data.sessions.iter().enumerate().skip(offset).take(visible) {
                        lines.push(format!(
                            "{} #{} {} {} 样本 {}",
                            if i == selection { ">" } else { " " },
                            s.id,
                            s.started_at,
                            s.saved,
                            if s.ended_at.is_none() {
                                "未结束（下载为部分快照）"
                            } else {
                                s.outcome.as_deref().unwrap_or("unknown")
                            }
                        ));
                    }
                    f.render_widget(
                        Paragraph::new(lines.join("\n"))
                            .block(Block::default().borders(Borders::ALL).title("历史会话")),
                        area,
                    );
                }
            })?;
        }
        if !event::poll(Duration::from_millis(100))? {
            continue;
        }
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }
        let action = settings.key(key);
        if action == ui::Action::RequestExit {
            break;
        }
        let command = match key.code {
            KeyCode::Char('h') => {
                history = !history;
                selection = 0;
                Some(Command::Sessions(None))
            }
            KeyCode::Esc => {
                history = false;
                None
            }
            KeyCode::Down if history => {
                selection =
                    (selection + 1).min(shared.lock().unwrap().sessions.len().saturating_sub(1));
                None
            }
            KeyCode::Up if history => {
                selection = selection.saturating_sub(1);
                None
            }
            KeyCode::PageDown if history => {
                selection = 0;
                shared
                    .lock()
                    .unwrap()
                    .sessions
                    .last()
                    .map(|s| Command::Sessions(Some(s.id)))
            }
            KeyCode::Char('r') => {
                refresh.store(true, Ordering::Relaxed);
                selection = 0;
                Some(if history {
                    Command::Sessions(None)
                } else {
                    Command::Refresh
                })
            }
            KeyCode::Char('d') => {
                let data = shared.lock().unwrap();
                let id = if history {
                    data.sessions.get(selection).map(|s| s.id)
                } else {
                    data.live.as_ref().and_then(|l| l.session_id)
                };
                id.map(Command::Download)
            }
            _ => None,
        };
        if let Some(command) = command {
            let downloading = matches!(command, Command::Download(_));
            let mut remote = shared.lock().unwrap();
            if downloading && remote.downloading {
                remote.message = "下载正在进行，请等待完成".into();
                continue;
            }
            if tx.try_send(command).is_err() {
                remote.message = "网络操作队列忙，请稍后重试".into();
            } else if downloading {
                remote.downloading = true;
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    drop(terminal);
    drop(_guard);
    drop(workers);
    Ok(())
}
