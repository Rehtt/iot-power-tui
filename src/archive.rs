//! Read-only, bounded history queries shared by local and remote viewers.
use anyhow::{ensure, Context, Result};
use ratatui::symbols::Marker;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct Query {
    pub from: Option<String>,
    pub to: Option<String>,
    pub points: Option<usize>,
}
impl Query {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=2000).contains(&self.points.unwrap_or(600)),
            "points must be 1..2000"
        );
        let a = self.from.as_deref().map(time).transpose()?;
        let b = self.to.as_deref().map(time).transpose()?;
        ensure!(
            !matches!((a,b), (Some(a),Some(b)) if a>b),
            "from must not exceed to"
        );
        Ok(())
    }
}
fn time(s: &str) -> Result<i64> {
    Ok(chrono::DateTime::parse_from_rfc3339(s)
        .context("invalid RFC3339 time")?
        .timestamp_micros())
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Point {
    pub ts: String,
    pub voltage_v: f64,
    pub current_a: f64,
    pub power_w: f64,
    pub samples: u64,
    pub min: [f64; 3],
    pub max: [f64; 3],
    pub break_before: bool,
}
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Statistics {
    pub average_voltage_v: f64,
    pub maximum_voltage_v: f64,
    pub average_current_a: f64,
    pub maximum_current_a: f64,
    pub average_power_w: f64,
    pub maximum_power_w: f64,
    pub duration_secs: f64,
    pub energy_wh: f64,
    pub samples: u64,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Response {
    pub session_id: i64,
    pub from: Option<String>,
    pub to: Option<String>,
    pub points: Vec<Point>,
    #[serde(default)]
    pub statistics: Statistics,
}
fn columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    Ok(conn
        .prepare(&format!("PRAGMA table_info({table})"))?
        .query_map([], |r| r.get(1))?
        .collect::<rusqlite::Result<_>>()?)
}
pub fn read(path: &Path, id: i64, query: &Query, cancel: &AtomicBool) -> Result<Response> {
    query.validate()?;
    let conn = crate::network::read_database(path)?;
    conn.execute_batch("BEGIN")?;
    ensure!(
        conn.query_row("SELECT count(*) FROM sessions WHERE id=?1", [id], |r| r
            .get::<_, i64>(0))?
            == 1,
        "session not found"
    );
    let names = columns(&conn, "measurements")?;
    let field = |name: &str, fallback: &str| {
        if names.iter().any(|s| s == name) {
            format!("coalesce({name},{fallback})")
        } else {
            fallback.to_string()
        }
    };
    let sql = format!("SELECT ts,voltage_v,current_a,power_w,{},{},{},{},{},{},{},{} FROM measurements WHERE session_id=?1 ORDER BY id", field("source_count","1"),field("end_ts","ts"),field("voltage_min","voltage_v"),field("current_min","current_a"),field("power_min","power_w"),field("voltage_max","voltage_v"),field("current_max","current_a"),field("power_max","power_w"));
    // Parse timestamps instead of comparing RFC3339 text (offsets and precision vary).
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([id])?;
    let (mut first, mut last) = (None::<i64>, None::<i64>);
    while let Some(row) = rows.next()? {
        ensure!(!cancel.load(Ordering::Relaxed), "history query cancelled");
        let t = time(&row.get::<_, String>(0)?)?;
        let end = time(&row.get::<_, String>(5)?)?;
        first = Some(first.map_or(t, |v| v.min(t)));
        last = Some(last.map_or(end, |v| v.max(end)));
    }
    drop(rows);
    let (Some(start), Some(end)) = (first, last) else {
        return Ok(Response {
            session_id: id,
            from: None,
            to: None,
            points: vec![],
            statistics: Statistics::default(),
        });
    };
    let start = query
        .from
        .as_deref()
        .map(time)
        .transpose()?
        .unwrap_or(start);
    let end = query.to.as_deref().map(time).transpose()?.unwrap_or(end);
    let span = end.saturating_sub(start).max(1);
    let capacity = query.points.unwrap_or(600);
    let mut buckets = BTreeMap::<usize, Point>::new();
    let mut rows = stmt.query([id])?;
    let mut previous = None::<i64>;
    while let Some(row) = rows.next()? {
        ensure!(!cancel.load(Ordering::Relaxed), "history query cancelled");
        let ts: String = row.get(0)?;
        let t = time(&ts)?;
        let until = time(&row.get::<_, String>(5)?)?;
        let count = row.get::<_, u64>(4)?.max(1);
        let gap = previous.is_some_and(|p| t < p || t.saturating_sub(p) > 100_000);
        previous = Some(until);
        if t < start || t > end {
            continue;
        }
        let index = (((t - start) as i128 * capacity as i128 / (span as i128 + 1)) as usize)
            .min(capacity - 1);
        let values = [row.get::<_, f64>(1)?, row.get(2)?, row.get(3)?];
        let min = [row.get::<_, f64>(6)?, row.get(7)?, row.get(8)?];
        let max = [row.get::<_, f64>(9)?, row.get(10)?, row.get(11)?];
        ensure!(
            values
                .iter()
                .chain(min.iter())
                .chain(max.iter())
                .all(|x| x.is_finite()),
            "invalid historical measurement"
        );
        if let Some(p) = buckets.get_mut(&index) {
            let total = p.samples + count;
            let old = [p.voltage_v, p.current_a, p.power_w];
            let means: [f64; 3] = std::array::from_fn(|i| {
                old[i] + (values[i] - old[i]) * count as f64 / total as f64
            });
            p.voltage_v = means[0];
            p.current_a = means[1];
            p.power_w = means[2];
            p.samples = total;
            for i in 0..3 {
                p.min[i] = p.min[i].min(min[i]);
                p.max[i] = p.max[i].max(max[i]);
            }
            p.break_before |= gap;
        } else {
            buckets.insert(
                index,
                Point {
                    ts,
                    voltage_v: values[0],
                    current_a: values[1],
                    power_w: values[2],
                    samples: count,
                    min,
                    max,
                    break_before: gap,
                },
            );
        }
    }
    let points: Vec<Point> = buckets.into_values().collect();
    let mut stats = Statistics {
        duration_secs: end.saturating_sub(start) as f64 / 1e6,
        maximum_voltage_v: f64::NEG_INFINITY,
        maximum_current_a: f64::NEG_INFINITY,
        maximum_power_w: f64::NEG_INFINITY,
        ..Statistics::default()
    };
    for p in &points {
        stats.samples += p.samples;
        stats.average_voltage_v += p.voltage_v * p.samples as f64;
        stats.average_current_a += p.current_a * p.samples as f64;
        stats.average_power_w += p.power_w * p.samples as f64;
        stats.maximum_voltage_v = stats.maximum_voltage_v.max(p.max[0]);
        stats.maximum_current_a = stats.maximum_current_a.max(p.max[1]);
        stats.maximum_power_w = stats.maximum_power_w.max(p.max[2]);
    }
    if stats.samples > 0 {
        let n = stats.samples as f64;
        stats.average_voltage_v /= n;
        stats.average_current_a /= n;
        stats.average_power_w /= n;
    }
    if stats.samples == 0 {
        stats.maximum_voltage_v = 0.0;
        stats.maximum_current_a = 0.0;
        stats.maximum_power_w = 0.0;
    }
    let energy_field = if names.iter().any(|s| s == "energy_wh") {
        "energy_wh"
    } else {
        "0"
    };
    let start_text = chrono::DateTime::from_timestamp_micros(start)
        .unwrap()
        .to_rfc3339();
    let end_text = chrono::DateTime::from_timestamp_micros(end)
        .unwrap()
        .to_rfc3339();
    stats.energy_wh = conn.query_row(&format!("SELECT coalesce((SELECT {energy_field} FROM measurements WHERE session_id=?1 AND ts<=?3 ORDER BY id DESC LIMIT 1) - (SELECT {energy_field} FROM measurements WHERE session_id=?1 AND ts>=?2 ORDER BY id LIMIT 1),0)"), rusqlite::params![id, start_text, end_text], |r| r.get::<_, f64>(0)).unwrap_or(0.0_f64).max(0.0);
    let stamp = |us| {
        chrono::DateTime::from_timestamp_micros(us)
            .unwrap()
            .to_rfc3339()
    };
    Ok(Response {
        session_id: id,
        from: Some(stamp(start)),
        to: Some(stamp(end)),
        points,
        statistics: stats,
    })
}

pub fn render(f: &mut ratatui::Frame<'_>, response: &Response, metric: usize) {
    use ratatui::{
        prelude::*,
        widgets::{Axis, Block, Borders, Chart, Clear, Dataset, GraphType, Paragraph},
    };
    let area = f.area();
    f.render_widget(Clear, area);
    let rows = Layout::vertical([Constraint::Min(4), Constraint::Length(5)]).split(area);
    let mut groups: Vec<Vec<(f64, f64)>> = vec![vec![]];
    let mut low = vec![];
    let mut high = vec![];
    let origin = response
        .from
        .as_deref()
        .and_then(|s| time(s).ok())
        .unwrap_or(0);
    let end = response
        .to
        .as_deref()
        .and_then(|s| time(s).ok())
        .unwrap_or(origin + 1);
    let mut bounds = [f64::INFINITY, f64::NEG_INFINITY];
    for p in &response.points {
        let x = (time(&p.ts).unwrap_or(origin) - origin) as f64 / 1e6;
        if p.break_before && !groups.last().unwrap().is_empty() {
            groups.push(vec![]);
        }
        let value = [p.voltage_v, p.current_a, p.power_w][metric];
        groups.last_mut().unwrap().push((x, value));
        low.push((x, p.min[metric]));
        high.push((x, p.max[metric]));
        bounds[0] = bounds[0].min(p.min[metric]);
        bounds[1] = bounds[1].max(p.max[metric]);
    }
    if !bounds[0].is_finite() {
        bounds = [0., 1.];
    }
    let pad = ((bounds[1] - bounds[0]) * 0.05)
        .max(bounds[0].abs() * 0.01)
        .max(1e-9);
    bounds = [bounds[0] - pad, bounds[1] + pad];
    let mut sets: Vec<_> = groups
        .iter()
        .map(|g| {
            Dataset::default()
                .data(g)
                .graph_type(GraphType::Line)
                .marker(Marker::Braille)
                .style(Color::Yellow)
        })
        .collect();
    sets.push(
        Dataset::default()
            .data(&low)
            .style(Color::LightBlue)
            .marker(Marker::Braille),
    );
    sets.push(
        Dataset::default()
            .data(&high)
            .style(Color::LightBlue)
            .marker(Marker::Braille),
    );
    let unit = ["V", "A", "W"][metric];
    let seconds = ((end - origin) as f64 / 1e6).max(1e-6);
    let xlabels =
        [0., seconds * 0.25, seconds * 0.5, seconds * 0.75, seconds].map(|v| format!("{v:.2}s"));
    let ylabels = [0., 0.25, 0.5, 0.75, 1.]
        .map(|v| format!("{:.4}", bounds[0] + (bounds[1] - bounds[0]) * v));
    f.render_widget(
        Chart::new(sets)
            .block(Block::default().borders(Borders::ALL).title(format!(
                "历史会话 #{} · {} · {} 桶",
                response.session_id,
                unit,
                response.points.len()
            )))
            .x_axis(
                Axis::default()
                    .bounds([0., seconds])
                    .labels(xlabels)
                    .title("相对时间"),
            )
            .y_axis(Axis::default().bounds(bounds).labels(ylabels).title(unit)),
        rows[0],
    );
    let s = &response.statistics;
    f.render_widget(Paragraph::new(format!("{} — {}\n平均电压 {:.4} V · 最高电压 {:.4} V · 平均电流 {:.4} A · 最高电流 {:.4} A\n平均功率 {:.4} W · 最高功率 {:.4} W · 总时长 {:.3} s · 总计电能 {:.6} Wh\n1/2/3 指标 · [/] 缩放 · ←/→ 移动 · Home 全部 · Esc 返回 · r 刷新",response.from.as_deref().unwrap_or("空会话"),response.to.as_deref().unwrap_or(""),s.average_voltage_v,s.maximum_voltage_v,s.average_current_a,s.maximum_current_a,s.average_power_w,s.maximum_power_w,s.duration_secs,s.energy_wh)),rows[1]);
}

/// A local browser owns only its terminal event loop; capture workers keep running.
pub fn browse(paths: Vec<std::path::PathBuf>) -> Result<()> {
    use crossterm::event::{self, Event, KeyCode, KeyEventKind};
    use ratatui::{
        backend::CrosstermBackend,
        widgets::{Block, Borders, Paragraph},
        Terminal,
    };
    let _guard = crate::TerminalGuard::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(std::io::stdout()))?;
    let mut rows = Vec::new();
    let mut notice = String::new();
    for path in paths {
        let result = (|| -> Result<()> {
            let conn = crate::network::read_database(&path)?;
            let cols = columns(&conn, "sessions")?;
            let name = if cols.iter().any(|c| c == "name") {
                "name"
            } else {
                "''"
            };
            let mut stmt = conn.prepare(&format!("SELECT id,{name},started_at,ended_at,(SELECT count(*) FROM measurements WHERE session_id=sessions.id) FROM sessions ORDER BY id DESC LIMIT 1000"))?;
            for row in stmt.query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<String>>(3)?,
                    r.get::<_, u64>(4)?,
                ))
            })? {
                let (id, name, start, end, count) = row?;
                let label = if name.is_empty() {
                    crate::storage::time_range_name(
                        &start,
                        end.as_deref().unwrap_or("采集中，仅已落盘数据"),
                    )
                } else {
                    name
                };
                rows.push((path.clone(), id, label, count));
            }
            Ok(())
        })();
        if let Err(e) = result {
            notice = format!("{}: {e:#}", path.display());
        }
    }
    let mut selection = 0usize;
    let mut delete_prompt: Option<crate::ui::DeletePrompt> = None;
    let mut delete_target: Option<(std::path::PathBuf, i64, String, u64)> = None;
    let mut delete_task: Option<std::thread::JoinHandle<Result<()>>> = None;
    let mut metric = 1;
    let mut response = None::<Response>;
    let mut task: Option<std::thread::JoinHandle<Result<Response>>> = None;
    let mut cancel = std::sync::Arc::new(AtomicBool::new(false));
    loop {
        if task.as_ref().is_some_and(|h| h.is_finished()) {
            match task.take().unwrap().join() {
                Ok(Ok(value)) => {
                    response = Some(value);
                    notice.clear();
                }
                Ok(Err(e)) => notice = format!("{e:#}"),
                Err(_) => notice = "历史查询线程失败".into(),
            }
        }
        if delete_task.as_ref().is_some_and(|h| h.is_finished()) {
            let target = delete_target.clone();
            let result = delete_task
                .take()
                .unwrap()
                .join()
                .map_err(|_| anyhow::anyhow!("删除线程失败"))?;
            match result {
                Ok(()) => {
                    if let Some((_, id, _, _)) = target {
                        rows.retain(|row| row.1 != id);
                        selection = selection.min(rows.len().saturating_sub(1));
                    }
                    delete_prompt = None;
                    delete_target = None;
                    notice = "删除成功，历史列表已更新".into();
                }
                Err(e) => {
                    notice = format!("删除失败：{e:#}");
                    if let Some(prompt) = delete_prompt.as_mut() {
                        prompt.busy = false;
                        prompt.error = Some(notice.clone());
                    }
                }
            }
        }
        terminal.draw(|f| {
            if let Some(data) = &response {
                render(f, data, metric);
            } else {
                let visible = f.area().height.saturating_sub(5) as usize;
                let offset = selection.saturating_sub(visible.saturating_sub(1));
                let mut text = format!("↑/↓ 选择 Enter 查看 d 删除 Esc 返回 · {}\n", notice);
                for (index, (path, id, label, count)) in
                    rows.iter().enumerate().skip(offset).take(visible)
                {
                    text.push_str(&format!(
                        "{} #{} {} · {} 条记录 [{}]\n",
                        if index == selection { ">" } else { " " },
                        id,
                        label,
                        count,
                        path.display()
                    ));
                }
                f.render_widget(
                    Paragraph::new(text)
                        .block(Block::default().borders(Borders::ALL).title("历史会话")),
                    f.area(),
                );
            }
            if let (Some(prompt), Some((_, id, name, count))) = (&delete_prompt, &delete_target) {
                prompt.render(f, *id, name, *count);
            }
        })?;
        if !event::poll(std::time::Duration::from_millis(100))? {
            continue;
        }
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }
        if task.is_none() && delete_task.is_none() {
            if let Some(query) = response.as_ref().and_then(|r| navigate(r, key.code)) {
                if let Some((path, id, _, _)) = rows.get(selection) {
                    let (path, id) = (path.clone(), *id);
                    cancel = std::sync::Arc::new(AtomicBool::new(false));
                    let token = cancel.clone();
                    task = Some(std::thread::spawn(move || read(&path, id, &query, &token)));
                }
                continue;
            }
        }
        if let Some(prompt) = delete_prompt.as_mut() {
            if let Some(action) = prompt.key(key) {
                match action {
                    crate::ui::DeleteChoice::Cancel => {
                        delete_prompt = None;
                        delete_target = None;
                    }
                    crate::ui::DeleteChoice::Confirm => {
                        if let Some((path, id, _, _)) = delete_target.clone() {
                            prompt.busy = true;
                            notice = format!("正在删除会话 #{id}…");
                            delete_task = Some(std::thread::spawn(move || {
                                let conn = crate::network::read_database(&path)?;
                                let ended: Option<String> = conn.query_row(
                                    "SELECT ended_at FROM sessions WHERE id=?1",
                                    [id],
                                    |r| r.get(0),
                                )?;
                                anyhow::ensure!(ended.is_some(), "活动会话不可删除，请先停止采集");
                                drop(conn);
                                crate::storage::delete_session(&path, id)
                            }));
                        }
                    }
                }
            }
            continue;
        }
        match key.code {
            KeyCode::Char('d') if response.is_none() => {
                if let Some(row) = rows.get(selection) {
                    delete_target = Some(row.clone());
                    delete_prompt = Some(crate::ui::DeletePrompt::new());
                }
            }
            KeyCode::Esc => {
                cancel.store(true, Ordering::Relaxed);
                // Wait only for the cancelled read worker; it checks cancellation per row.
                if let Some(h) = task.take() {
                    let _ = h.join();
                }
                if response.take().is_none() {
                    break;
                }
            }
            KeyCode::Char('q') => {
                cancel.store(true, Ordering::Relaxed);
                break;
            }
            KeyCode::Up if response.is_none() => selection = selection.saturating_sub(1),
            KeyCode::Down if response.is_none() => {
                selection = (selection + 1).min(rows.len().saturating_sub(1))
            }
            KeyCode::Char(c @ '1'..='3') => metric = c as usize - '1' as usize,
            KeyCode::Enter | KeyCode::Char('r') if task.is_none() => {
                if let Some((path, id, _, _)) = rows.get(selection) {
                    let (path, id) = (path.clone(), *id);
                    cancel = std::sync::Arc::new(AtomicBool::new(false));
                    let token = cancel.clone();
                    notice = "读取历史中…".into();
                    task = Some(std::thread::spawn(move || {
                        read(&path, id, &Query::default(), &token)
                    }));
                }
            }
            _ => {}
        }
    }
    cancel.store(true, Ordering::Relaxed);
    if let Some(h) = task {
        let _ = h.join();
    }
    Ok(())
}

pub fn navigate(response: &Response, key: crossterm::event::KeyCode) -> Option<Query> {
    use crossterm::event::KeyCode;
    if key == KeyCode::Home {
        return Some(Query::default());
    }
    let (mut a, mut b) = (
        time(response.from.as_deref()?).ok()?,
        time(response.to.as_deref()?).ok()?,
    );
    let span = (b - a).max(2);
    match key {
        KeyCode::Char('[') => {
            a += span / 4;
            b -= span / 4;
        }
        KeyCode::Char(']') => {
            a = a.saturating_sub(span / 2);
            b = b.saturating_add(span / 2);
        }
        KeyCode::Left => {
            a = a.saturating_sub(span / 2);
            b = b.saturating_sub(span / 2);
        }
        KeyCode::Right => {
            a = a.saturating_add(span / 2);
            b = b.saturating_add(span / 2);
        }
        KeyCode::Char('r') => {}
        _ => return None,
    }
    Some(Query {
        from: Some(chrono::DateTime::from_timestamp_micros(a)?.to_rfc3339()),
        to: Some(chrono::DateTime::from_timestamp_micros(b)?.to_rfc3339()),
        points: Some(600),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_session_is_weighted_bounded_and_read_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE sessions(id INTEGER PRIMARY KEY); INSERT INTO sessions VALUES(1); CREATE TABLE measurements(id INTEGER PRIMARY KEY,session_id INTEGER,ts TEXT,voltage_v REAL,current_a REAL,power_w REAL,source_count INTEGER,voltage_max REAL); INSERT INTO measurements VALUES(1,1,'2026-01-01T00:00:00Z',1,2,3,1,9),(2,1,'2026-01-01T00:00:01Z',3,4,5,3,3);").unwrap();
        drop(conn);
        let before = std::fs::read(&path).unwrap();
        let result = read(
            &path,
            1,
            &Query {
                points: Some(1),
                ..Default::default()
            },
            &AtomicBool::new(false),
        )
        .unwrap();
        assert_eq!(result.points.len(), 1);
        assert_eq!(result.points[0].samples, 4);
        assert_eq!(result.points[0].voltage_v, 2.5);
        assert_eq!(result.points[0].max[0], 9.0);
        assert_eq!(result.statistics.samples, 4);
        assert_eq!(result.statistics.average_voltage_v, 2.5);
        assert_eq!(result.statistics.maximum_voltage_v, 9.0);
        assert_eq!(result.statistics.duration_secs, 1.0);
        assert!(result.points[0].break_before);
        assert_eq!(before, std::fs::read(&path).unwrap());
        assert!(read(&path, 2, &Query::default(), &AtomicBool::new(false)).is_err());
        assert!(read(&path, 1, &Query::default(), &AtomicBool::new(true)).is_err());
    }
}
