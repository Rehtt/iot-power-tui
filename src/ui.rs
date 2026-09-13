use crate::{
    history,
    runtime::{Shared, State},
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    prelude::*,
    widgets::{Axis, Block, Borders, Chart, Clear, Dataset, GraphType, Paragraph, Wrap},
};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Finish {
    Save,
    Discard,
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Action {
    None,
    RequestExit,
    Finish(Finish),
    StopRestart,
    Reset,
    Configure,
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SessionChoice { Save, Discard }
pub struct SessionPrompt { pub choice: usize, pub name: String, pub closed: bool, replace: bool }
impl SessionPrompt {
    pub fn new(name: String) -> Self { Self { choice: 0, name, closed: false, replace: true } }
    pub fn key(&mut self, key: KeyEvent) -> Option<SessionChoice> {
        if key.code == KeyCode::Esc { self.closed = true; return Some(SessionChoice::Discard); }
        match key.code {
            KeyCode::Up | KeyCode::Left | KeyCode::BackTab | KeyCode::Down | KeyCode::Right | KeyCode::Tab => self.choice = 1 - self.choice,
            KeyCode::Char('n') if self.replace => return Some(SessionChoice::Discard),
            KeyCode::Char('y') if self.replace => self.choice = 0,
            KeyCode::Enter if self.choice == 1 => return Some(SessionChoice::Discard),
            KeyCode::Enter => { let n=self.name.trim().to_string(); if !n.is_empty() && n.len()<=128 && !n.chars().any(char::is_control) { return Some(SessionChoice::Save); } }
            KeyCode::Backspace => { if self.replace { self.name.clear(); self.replace=false; } else { self.name.pop(); } }
            KeyCode::Char(c) if !c.is_control() => { if self.replace { self.name.clear(); self.replace=false; } if self.name.len()+c.len_utf8()<=128 { self.name.push(c); } }
            _ => {}
        }
        None
    }
    pub fn render(&self, f: &mut Frame<'_>) { modal(f, " 会话切换 ", &format!("保存旧会话并创建新会话？\n名称：{}\n输入名称后按 Enter 保存；n 直接丢弃。\n名称长度 1–128 字节。", self.name), Some(self.choice)); }
}
pub struct UiState {
    pub metric: usize,
    pub window: usize,
    pub dialog: Option<usize>,
}
impl Default for UiState {
    fn default() -> Self {
        Self {
            metric: 1,
            window: 1,
            dialog: None,
        }
    }
}
impl UiState {
    pub fn seconds(&self) -> u32 {
        [10, 30, 60][self.window]
    }
    pub fn key(&mut self, key: KeyEvent) -> Action {
        let exit = key.code == KeyCode::Char('q')
            || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL));
        if let Some(choice) = self.dialog.as_mut() {
            if exit || key.code == KeyCode::Esc {
                self.dialog = None;
                return Action::None;
            }
            return match key.code {
                KeyCode::Up | KeyCode::Left | KeyCode::BackTab => {
                    *choice = (*choice + 2) % 3;
                    Action::None
                }
                KeyCode::Down | KeyCode::Right | KeyCode::Tab => {
                    *choice = (*choice + 1) % 3;
                    Action::None
                }
                KeyCode::Char('y') => Action::Finish(Finish::Save),
                KeyCode::Char('n') => Action::Finish(Finish::Discard),
                KeyCode::Enter => match *choice {
                    0 => Action::Finish(Finish::Save),
                    1 => Action::Finish(Finish::Discard),
                    _ => {
                        self.dialog = None;
                        Action::None
                    }
                },
                _ => Action::None,
            };
        }
        if exit {
            return Action::RequestExit;
        }
        match key.code {
            KeyCode::Char('1') => self.metric = 0,
            KeyCode::Char('2') => self.metric = 1,
            KeyCode::Char('3') => self.metric = 2,
            KeyCode::Char('[') => self.window = self.window.saturating_sub(1),
            KeyCode::Char(']') => self.window = (self.window + 1).min(2),
            KeyCode::Char('r') => return Action::Reset,
            KeyCode::Char('s') => return Action::StopRestart,
            KeyCode::Char('c') => return Action::Configure,
            _ => {}
        }
        Action::None
    }
}
#[derive(Clone, Default)]
pub struct Totals {
    pub sessions: u64,
    pub received: u64,
    pub accepted: u64,
    pub staged: u64,
    pub gaps: u64,
    pub invalid: u64,
    pub dropped: u64,
    pub error: Option<String>,
}
impl Totals {
    pub fn add(&mut self, s: &Shared) {
        self.sessions += u64::from(s.device.is_some());
        self.received += s.received;
        self.accepted += s.accepted;
        self.staged += s.saved;
        self.gaps += s.gaps;
        self.invalid += s.invalid;
        self.dropped += s.dropped;
        if let Some(e) = &s.error {
            self.error = Some(e.clone());
        }
    }
}
#[derive(Clone, Default)]
pub struct Progress {
    pub message: String,
    pub done: u64,
    pub total: u64,
}
pub struct View<'a> {
    pub state: &'a Shared,
    pub totals: &'a Totals,
    pub settings: &'a UiState,
    pub rate: f64,
    pub target: &'a str,
    pub remote: bool,
    pub cache: &'a str,
    pub progress: Option<&'a Progress>,
    pub notice: Option<&'a str>,
}
fn panel(title: impl Into<Line<'static>>) -> Block<'static> {
    Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
}
fn state_label(state: State) -> (&'static str, Color) {
    match state {
        State::Connecting => ("连接中", Color::Yellow),
        State::Calibrating => ("读取校准", Color::Yellow),
        State::Capturing => ("采集中", Color::Green),
        State::Stopped => ("已停止", Color::Gray),
        State::Fault => ("故障", Color::Red),
    }
}
pub fn render(f: &mut Frame<'_>, v: View<'_>) {
    let area = f.area();
    if area.width < 48 || area.height < 16 {
        f.render_widget(
            Paragraph::new(if v.remote {
                "请调整终端至至少 48 列 × 16 行\n[q / Ctrl+C] 退出客户端"
            } else {
                "请调整终端至至少 48 列 × 16 行\n[q / Ctrl+C] 退出并选择保存"
            })
            .wrap(Wrap { trim: false }),
            area,
        );
    } else {
        let compact = area.height < 27;
        let rows = Layout::vertical([
            Constraint::Length(3),
            Constraint::Length(if compact { 3 } else { 5 }),
            Constraint::Min(5),
            Constraint::Length(if compact { 3 } else { 5 }),
            Constraint::Length(2),
        ])
        .split(area);
        let (label, color) = state_label(v.state.state);
        let device = v.state.device.as_deref().unwrap_or("等待设备");
        f.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled(format!(" {label} "), Style::default().fg(color).bold()),
                Span::raw(format!("{device}   {:.0} samples/s", v.rate)),
            ]))
            .block(panel(if v.remote {
                format!(" IoT Power CC · {} ", v.target)
            } else {
                " IoT Power CC ".into()
            })),
            rows[0],
        );
        let text = if let Some(m) = &v.state.metrics.latest {
            if compact {
                format!(
                    "U {:.4} V  I {:.6} A  P {:.6} W",
                    m.voltage_v, m.current_a, m.power_w
                )
            } else {
                format!("电压 {:>10.5} V    电流 {:>12.8} A    功率 {:>12.8} W\n会话能量 {:.10} Wh    显示样本 {}\n平均电流 {:.8} A    平均功率 {:.8} W    峰值功率 {:.8} W",m.voltage_v,m.current_a,m.power_w,m.energy_wh,v.state.metrics.count,v.state.metrics.average_current,v.state.metrics.average_power,v.state.metrics.peak_power)
            }
        } else {
            "等待测量数据…".into()
        };
        f.render_widget(
            Paragraph::new(text)
                .block(panel(" 实时测量 "))
                .wrap(Wrap { trim: false }),
            rows[1],
        );
        let mut series = v.state.history.series(
            v.settings.metric,
            v.settings.seconds(),
            rows[2].width.saturating_sub(14) as usize,
        );
        let (scale, unit, bounds) = history::axis(&series, v.settings.metric);
        for s in &mut series {
            for p in s.mean.iter_mut().chain(&mut s.min).chain(&mut s.max) {
                p.1 *= scale;
            }
        }
        let mut datasets = Vec::new();
        for (i, s) in series.iter().enumerate() {
            for (name, color, data) in [
                ("最小", Color::Blue, &s.min),
                ("最大", Color::LightBlue, &s.max),
                ("平均", Color::Yellow, &s.mean),
            ] {
                let mut d = Dataset::default()
                    .marker(symbols::Marker::Braille)
                    .graph_type(GraphType::Line)
                    .style(Style::default().fg(color))
                    .data(data);
                if i == 0 {
                    d = d.name(name);
                }
                datasets.push(d);
            }
        }
        let metric = ["电压", "电流", "功率"][v.settings.metric];
        let chart = Chart::new(datasets)
            .block(panel(Line::from(vec![
                Span::raw(if compact {
                    format!(
                        " {metric}趋势 · {}s · {unit} · 缓存{:.1}/{:.0}M×2 待{} ",
                        v.settings.seconds(),
                        v.state.buffered_bytes as f64 / 1e6,
                        v.state.config.buffer_size_bytes as f64 / 1e6,
                        v.state.records.saturating_sub(v.state.saved)
                    )
                } else {
                    format!(" {metric}趋势 · {} s · {unit}  ", v.settings.seconds())
                }),
                Span::styled("均值", Style::default().fg(Color::Yellow)),
                Span::raw(" / "),
                Span::styled("极值 ", Style::default().fg(Color::LightBlue)),
            ])))
            .hidden_legend_constraints((Constraint::Length(0), Constraint::Length(0)))
            .x_axis(
                Axis::default()
                    .bounds([-(v.settings.seconds() as f64), 0.0])
                    .labels([
                        format!("-{}s", v.settings.seconds()),
                        format!("-{}s", v.settings.seconds() / 2),
                        "0s".into(),
                    ])
                    .style(Style::default().fg(Color::DarkGray)),
            )
            .y_axis(
                Axis::default()
                    .bounds(bounds)
                    .labels([
                        format!("{:.3}", bounds[0]),
                        format!("{:.3}", (bounds[0] + bounds[1]) / 2.0),
                        format!("{:.3}", bounds[1]),
                    ])
                    .style(Style::default().fg(Color::DarkGray)),
            );
        f.render_widget(chart, rows[2]);
        let mut footer = format!(
            "本次 {} 会话  接受 {}  {} {}  丢包 {}  无效 {}  丢样 {}",
            v.totals.sessions,
            v.totals.accepted,
            if v.remote {
                "服务端已存记录"
            } else {
                "已暂存记录"
            },
            v.totals.staged,
            v.totals.gaps,
            v.totals.invalid,
            v.totals.dropped
        );
        if compact {
            footer = format!(
                "接受 {} · 已存 {} · 丢包 {} · 无效 {} · 丢样 {}",
                v.totals.accepted,
                v.totals.staged,
                v.totals.gaps,
                v.totals.invalid,
                v.totals.dropped
            );
        }
        footer.push_str(&format!(
            "\n记录 {} Hz · 缓存 {:.2}/{:.2} MB ×2 · 待写 {} 条 · 写盘 {:.2} MB",
            v.state.config.sample_rate_hz,
            v.state.buffered_bytes as f64 / 1e6,
            v.state.config.buffer_size_bytes as f64 / 1e6,
            v.state.records.saturating_sub(v.state.saved),
            v.state.writing_bytes as f64 / 1e6
        ));
        if let Some(error) = v.notice.or(v.totals.error.as_deref()) {
            if compact {
                footer = if v.remote {
                    format!("服务端已保存 {} | {error}", v.totals.staged)
                } else {
                    format!("异常：{error}")
                };
            } else {
                footer.push_str(&format!("\n{error}"));
            }
        } else if !compact {
            footer.push_str(if v.remote {"\n详细数据保存在服务端；[h] 历史会话 [d] 下载 SQLite"}else{"\n数据尚未写入最终数据库，退出时选择保存。\n图表保留最小/最大值；USB 时间为估算时间。"});
        }
        f.render_widget(
            Paragraph::new(footer)
                .wrap(Wrap { trim: false })
                .block(panel(" 采集与保存 ")),
            rows[3],
        );
        f.render_widget(Paragraph::new(if v.remote {"[1/2/3] 电压/电流/功率  [ / ] 时间窗\n[c] 设置 [h] 会话 [d] 下载 [r] 刷新 [q/Ctrl+C] 退出客户端"}else{"[1/2/3] 电压/电流/功率  [ / ] 时间窗\n[c] 设置 [s] 停止/新会话  [r] 重置显示  [q/Ctrl+C] 退出"}).style(Style::default().fg(Color::Cyan)),rows[4]);
    }
    if let Some(progress) = v.progress {
        let text = format!(
            "{}\n{} / {} 记录\n请等待操作完成…",
            progress.message, progress.done, progress.total
        );
        modal(f, " 处理中 ", &text, None);
    } else if let Some(choice) = v.settings.dialog {
        let mut text=format!("是否保存本次启动期间的数据？\n{} 个会话，{} 个已接受样本，{} 条已暂存记录\n目标：{}\n\n确认前采集继续；选择后停止并处理全部会话。",v.totals.sessions,v.totals.accepted,v.totals.staged,v.target);
        if let Some(notice) = v.notice.or(v.totals.error.as_deref()) {
            text.push_str(&format!("\n\n{notice}\n缓存保留于：{}", v.cache));
        }
        modal(f, " 退出 · 数据保存 ", &text, Some(choice));
    }
}
fn modal(f: &mut Frame<'_>, title: &str, text: &str, choice: Option<usize>) {
    let area = f.area();
    let width = area.width.saturating_sub(4).clamp(1, 100).min(area.width);
    let height = area
        .height
        .saturating_sub(2)
        .min(if choice.is_some() {
            20
        } else if title.contains("设置") {
            12
        } else {
            8
        })
        .max(1)
        .min(area.height);
    let rect = Rect::new(
        area.x + (area.width - width) / 2,
        area.y + (area.height - height) / 2,
        width,
        height,
    );
    f.render_widget(Clear, rect);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .border_style(Style::default().fg(Color::Cyan));
    let inner = block.inner(rect);
    f.render_widget(block, rect);
    let rows = Layout::vertical([
        Constraint::Min(0),
        Constraint::Length(if choice.is_some() { 5 } else { 0 }),
    ])
    .split(inner);
    f.render_widget(Paragraph::new(text).wrap(Wrap { trim: false }), rows[0]);
    if let Some(choice) = choice {
        let mut lines = Vec::new();
        for (i, label) in ["[y] 保存并退出", "[n] 不保存退出", "[Esc] 取消"]
            .iter()
            .enumerate()
        {
            lines.push(Line::styled(
                format!("{} {label}", if i == choice { ">" } else { " " }),
                if i == choice {
                    Style::default().bg(Color::Cyan).fg(Color::Black)
                } else {
                    Style::default()
                },
            ));
        }
        lines.push(Line::raw("↑↓ / ←→ / Tab 选择，Enter 确认"));
        f.render_widget(Paragraph::new(lines), rows[1]);
    }
}
/// The same validated editor is used locally and by the remote client.
pub struct ConfigEditor {
    pub buffer: String,
    pub rate: String,
    pub field: usize,
    pub error: Option<String>,
    pub closed: bool,
    replace: bool,
}
impl ConfigEditor {
    pub fn new(config: crate::recording::Config) -> Self {
        Self {
            buffer: config.buffer_size_bytes.to_string(),
            rate: config.sample_rate_hz.to_string(),
            field: 0,
            error: None,
            closed: false,
            replace: true,
        }
    }
    pub fn key(&mut self, key: KeyEvent) -> Option<crate::recording::Config> {
        if key.code == KeyCode::Esc
            || key.code == KeyCode::Char('q')
            || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
        {
            self.closed = true;
            return None;
        }
        if matches!(
            key.code,
            KeyCode::Tab | KeyCode::BackTab | KeyCode::Up | KeyCode::Down
        ) {
            self.field = 1 - self.field;
            self.replace = true;
            return None;
        }
        if key.code == KeyCode::Enter {
            let result = (|| -> anyhow::Result<crate::recording::Config> {
                let buffer_size_bytes =
                    crate::recording::parse_size(&self.buffer).map_err(anyhow::Error::msg)?;
                crate::recording::Config {
                    buffer_size_bytes,
                    sample_rate_hz: self.rate.parse()?,
                }
                .validate()
            })();
            match result {
                Ok(c) => return Some(c),
                Err(e) => self.error = Some(e.to_string()),
            };
            return None;
        }
        let value = if self.field == 0 {
            &mut self.buffer
        } else {
            &mut self.rate
        };
        match key.code {
            KeyCode::Char('a') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                value.clear();
                self.replace = false;
            }
            KeyCode::Backspace => {
                if self.replace {
                    value.clear();
                } else {
                    value.pop();
                }
                self.replace = false;
            }
            KeyCode::Char(c)
                if c.is_ascii_alphanumeric() && !key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                if self.replace {
                    value.clear();
                    self.replace = false;
                }
                if value.len() < 20 {
                    value.push(c);
                }
            }
            _ => {}
        }
        None
    }
    pub fn render(&self, f: &mut Frame<'_>) {
        let text=format!("{} 每份缓存：{} 字节（可输入 10M / 10MiB）\n{} 软件记录速率：{} Hz（1–10000）\n\n双缓冲约使用两倍容量；设备仍接收完整数据。\n应用会先保存旧缓存，再开始新会话。\nTab 切换字段 · 输入替换 · Enter 应用 · Esc 取消\n{}",if self.field==0 {">"}else{" "},self.buffer,if self.field==1 {">"}else{" "},self.rate,self.error.as_deref().unwrap_or(""));
        modal(f, " 采集设置 ", &text, None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;
    #[test]
    fn settings_editor_validates_applies_and_cancels() {
        let mut editor = ConfigEditor::new(crate::recording::Config::default());
        let key = |code| KeyEvent::new(code, KeyModifiers::NONE);
        for c in "10MiB".chars() {
            editor.key(key(KeyCode::Char(c)));
        }
        editor.key(key(KeyCode::Tab));
        editor.key(key(KeyCode::Char('0')));
        assert!(editor.key(key(KeyCode::Enter)).is_none());
        assert!(editor.error.is_some());
        editor.key(key(KeyCode::Backspace));
        for c in "333".chars() {
            editor.key(key(KeyCode::Char(c)));
        }
        let config = editor.key(key(KeyCode::Enter)).unwrap();
        assert_eq!(config.sample_rate_hz, 333);
        assert_eq!(config.buffer_size_bytes, 10_485_760);
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        terminal.draw(|f| editor.render(f)).unwrap();
        assert!(terminal.backend().to_string().contains("采集设置"));
        editor.key(key(KeyCode::Esc));
        assert!(editor.closed);
    }
    #[test]
    fn remote_layout_identifies_service_storage_without_local_save_actions() {
        let mut terminal = Terminal::new(TestBackend::new(140, 32)).unwrap();
        let state = Shared::default();
        let totals = Totals::default();
        let settings = UiState::default();
        terminal
            .draw(|f| {
                render(
                    f,
                    View {
                        state: &state,
                        totals: &totals,
                        settings: &settings,
                        rate: 0.0,
                        target: "http://127.0.0.1:8080",
                        remote: true,
                        cache: "",
                        progress: None,
                        notice: Some("离线：保留最后图表"),
                    },
                )
            })
            .unwrap();
        let text = terminal.backend().to_string();
        assert!(text.contains("127.0.0.1:8080"));
        assert!(text.contains("服务端已存记录"));
        assert!(text.contains("离线"));
        assert!(text.contains("退出客户端"));
        assert!(!text.contains("停止/新会话"));
        assert!(!text.contains("已暂存记录"));
    }
    #[test]
    fn keys_select_metrics_windows_and_cancel_without_exit() {
        let mut ui = UiState::default();
        assert_eq!(ui.metric, 1);
        assert_eq!(ui.seconds(), 30);
        ui.key(KeyCode::Char('3').into());
        assert_eq!(ui.metric, 2);
        ui.key(KeyCode::Char(']').into());
        assert_eq!(ui.seconds(), 60);
        ui.dialog = Some(0);
        assert_eq!(ui.key(KeyCode::Char('q').into()), Action::None);
        assert!(ui.dialog.is_none());
        ui.dialog = Some(0);
        assert_eq!(
            ui.key(KeyCode::Char('n').into()),
            Action::Finish(Finish::Discard)
        );
        for key in [KeyCode::Up, KeyCode::Left, KeyCode::BackTab] {
            ui.dialog = Some(0);
            assert_eq!(ui.key(key.into()), Action::None);
            assert_eq!(ui.dialog, Some(2));
            assert_eq!(ui.key(KeyCode::Enter.into()), Action::None);
            assert!(ui.dialog.is_none());
        }
        for key in [KeyCode::Down, KeyCode::Right, KeyCode::Tab] {
            ui.dialog = Some(0);
            assert_eq!(ui.key(key.into()), Action::None);
            assert_eq!(ui.dialog, Some(1));
            assert_eq!(
                ui.key(KeyCode::Enter.into()),
                Action::Finish(Finish::Discard)
            );
            ui.dialog = Some(2);
            ui.key(key.into());
            assert_eq!(ui.dialog, Some(0));
            assert_eq!(ui.key(KeyCode::Enter.into()), Action::Finish(Finish::Save));
        }
    }
    #[test]
    fn renders_responsive_layout_dialog_and_tiny_terminal() {
        for (width, height) in [(140, 40), (80, 24), (48, 16), (20, 8)] {
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            let s = Shared::default();
            let t = Totals::default();
            let mut settings = UiState::default();
            terminal
                .draw(|f| {
                    render(
                        f,
                        View {
                            state: &s,
                            totals: &t,
                            settings: &settings,
                            rate: 0.0,
                            target: "final.db",
                            remote: false,
                            cache: "cache.db",
                            progress: None,
                            notice: None,
                        },
                    )
                })
                .unwrap();
            let text = terminal.backend().to_string();
            assert!(text.contains(if width < 48 {
                "请调整终端"
            } else {
                "电流趋势"
            }));
            settings.dialog = Some(0);
            terminal
                .draw(|f| {
                    render(
                        f,
                        View {
                            state: &s,
                            totals: &t,
                            settings: &settings,
                            rate: 0.0,
                            target: "final.db",
                            remote: false,
                            cache: "cache.db",
                            progress: None,
                            notice: None,
                        },
                    )
                })
                .unwrap();
            if width >= 48 && height >= 24 {
                let text = terminal.backend().to_string();
                assert!(text.contains("保存并退出"), "{width}x{height}: {text}");
                assert!(text.contains("不保存退出"), "{width}x{height}: {text}");
            }
        }
    }
    #[test]
    fn renders_sampled_spikes_and_keeps_error_actions_visible() {
        let mut s = Shared {
            state: State::Capturing,
            device: Some("SYNTHETIC-CC".into()),
            ..Shared::default()
        };
        for i in 0..3000 {
            let current =
                0.04 + (i as f64 / 130.0).sin() * 0.025 + if i % 701 == 0 { 0.15 } else { 0.0 };
            let m = crate::domain::Measurement {
                timestamp: chrono::DateTime::from_timestamp_micros(i * 10_000).unwrap(),
                device_id: "SYNTHETIC-CC".into(),
                voltage_v: 5.0,
                current_a: current,
                power_w: 5.0 * current,
                energy_wh: 0.0004,
                status: "synthetic".into(),
                raw: vec![],
            };
            std::sync::Arc::make_mut(&mut s.history).observe(&m);
            std::sync::Arc::make_mut(&mut s.metrics).observe(m);
        }
        s.accepted = 3000;
        s.received = 3000;
        s.saved = 3000;
        let mut totals = Totals::default();
        totals.add(&s);
        let mut settings = UiState::default();
        for (name, width, height, error) in [
            ("chart", 120, 36, None),
            (
                "exit",
                80,
                24,
                Some("保存失败：目标目录不可写。临时数据保留，可重试或选择不保存。"),
            ),
        ] {
            if error.is_some() {
                settings.dialog = Some(0);
            }
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            terminal
                .draw(|f| {
                    render(
                        f,
                        View {
                            state: &s,
                            totals: &totals,
                            settings: &settings,
                            rate: 100.0,
                            target: "data/iot-power.db",
                            remote: false,
                            cache: "data/.iot-power-pending/capture-example/capture.db",
                            progress: None,
                            notice: error,
                        },
                    )
                })
                .unwrap();
            let text = terminal.backend().to_string();
            if error.is_some() {
                assert!(
                    text.contains("保存并退出")
                        && text.contains("不保存退出")
                        && text.contains("取消")
                );
            } else {
                assert!(text.contains("mA"));
                assert!(text.chars().any(|c| ('⠁'..='⣿').contains(&c)));
            }
            if let Ok(directory) = std::env::var("IOT_POWER_SNAPSHOT_DIR") {
                let directory = std::path::Path::new(&directory);
                std::fs::create_dir_all(directory).unwrap();
                let cells=terminal.backend().buffer().content.iter().map(|c|serde_json::json!({"symbol":c.symbol(),"fg":format!("{:?}",c.fg),"bg":format!("{:?}",c.bg)})).collect::<Vec<_>>();
                let snapshot = serde_json::json!({"width":width,"height":height,"cells":cells});
                std::fs::write(
                    directory.join(format!("{name}.json")),
                    serde_json::to_vec(&snapshot).unwrap(),
                )
                .unwrap();
            }
        }
    }
}
