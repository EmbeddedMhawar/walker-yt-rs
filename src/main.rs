use anyhow::{anyhow, Result};
use clap::Parser;
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{self, Stdio};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::time::{sleep, Duration};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(trailing_var_arg = true)]
    query: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct Video {
    title: String,
    channel: String,
    id: String,
    thumbnail: String,
}

const CACHE_DIR: &str = "~/.cache/walker-yt-rs";
const DEMUCS_BIN: &str = "~/.local/share/walker-yt/venv/bin/demucs";
const YT_DLP_BIN: &str = "~/.local/bin/yt-dlp";

fn expand_path(path: &str) -> PathBuf {
    PathBuf::from(shellexpand::tilde(path).into_owned())
}

fn notify(title: &str, body: &str, urgency: &str) {
    let _ = process::Command::new("notify-send")
        .args(["-u", urgency, title, body, "-h", "string:x-canonical-private-synchronous:walker-yt"])
        .spawn();
}

async fn walker_dmenu(prompt: &str, lines: Vec<String>) -> Result<String> {
    let mut child = Command::new("walker")
        .args(["-d", "-p", prompt])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;

    let mut stdin = child.stdin.take().ok_or_else(|| anyhow!("Failed to open stdin"))?;
    stdin.write_all(lines.join("\n").as_bytes()).await?;
    drop(stdin);

    let output = child.wait_with_output().await?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

async fn search_youtube(query: &str) -> Result<Vec<Video>> {
    let yt_dlp = expand_path(YT_DLP_BIN);
    let output = Command::new(yt_dlp)
        .args([
            "ytsearch10:".to_string() + query,
            "--print".to_string(),
            "%(title)s\t%(channel)s\t%(id)s\t%(thumbnail)s".to_string(),
            "--no-playlist".to_string(),
            "--no-warnings".to_string(),
            "--ignore-config".to_string(),
        ])
        .output()
        .await?;

    let mut videos = Vec::new();
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() >= 4 {
            videos.push(Video {
                title: parts[0].to_string(),
                channel: parts[1].to_string(),
                id: parts[2].to_string(),
                thumbnail: parts[3].to_string(),
            });
        }
    }
    Ok(videos)
}

async fn get_video_qualities(video_id: &str) -> Result<Vec<String>> {
    let yt_dlp = expand_path(YT_DLP_BIN);
    let output = Command::new(yt_dlp)
        .args([
            "--dump-json",
            "--no-playlist",
            &format!("https://www.youtube.com/watch?v={}", video_id),
        ])
        .output()
        .await?;

    let data: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let formats = data["formats"].as_array().ok_or_else(|| anyhow!("No formats found"))?;
    
    let mut quality_map: HashMap<u64, Vec<u64>> = HashMap::new();
    for f in formats {
        if let (Some(h), Some(vcodec)) = (f["height"].as_u64(), f["vcodec"].as_str()) {
            if vcodec != "none" {
                let fps = f["fps"].as_u64().unwrap_or(0);
                quality_map.entry(h).or_default().push(fps);
            }
        }
    }

    let mut options = Vec::new();
    let mut heights: Vec<_> = quality_map.keys().collect();
    heights.sort_by(|a, b| b.cmp(a));

    for h in heights {
        let mut fps_list = quality_map[h].clone();
        fps_list.sort_by(|a, b| b.cmp(a));
        fps_list.dedup();
        for fps in fps_list {
            if fps >= 50 || (fps > 0 && quality_map[h].len() == 1) {
                options.push(format!("{}p{}", h, fps));
            } else if fps == 30 && !quality_map[h].contains(&60) {
                options.push(format!("{}p{}", h, fps));
            } else if fps == 0 {
                options.push(format!("{}p", h));
            }
        }
    }
    Ok(options)
}

async fn cleanup_old_cache() -> Result<()> {
    let cache_dir = expand_path(CACHE_DIR);
    if !cache_dir.exists() { return Ok(()); }

    let mut entries: Vec<_> = fs::read_dir(&cache_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map_or(false, |ft| ft.is_dir()))
        .collect();

    // Sort by modified time (newest first)
    entries.sort_by(|a, b| {
        let ma = a.metadata().and_then(|m| m.modified()).ok();
        let mb = b.metadata().and_then(|m| m.modified()).ok();
        mb.cmp(&ma)
    });

    // Keep only top 5
    if entries.len() > 5 {
        for entry in entries.iter().skip(5) {
            let _ = fs::remove_dir_all(entry.path());
        }
    }

    Ok(())
}

async fn get_subtitles(video_id: &str) -> Result<Vec<String>> {
    let yt_dlp = expand_path(YT_DLP_BIN);
    let output = Command::new(yt_dlp)
        .args([
            "--list-subs",
            "--quiet",
            &format!("https://www.youtube.com/watch?v={}", video_id),
        ])
        .output()
        .await?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut subs = Vec::new();
    let mut start_parsing = false;

    for line in stdout.lines() {
        // Start parsing if we see the table header
        if line.contains("Language") && line.contains("Name") && line.contains("Formats") {
            start_parsing = true;
            continue;
        }
        
        // Stop parsing if we hit a different section or empty line
        if start_parsing && line.is_empty() {
            start_parsing = false;
        }

        if start_parsing {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let code = parts[0];
                let name = parts[1..parts.len() - 1].join(" ");
                let name = if name.is_empty() { code } else { &name };
                subs.push(format!("{} ({})", name, code));
            }
        }
    }
    
    subs.sort();
    subs.dedup();
    Ok(subs)
}

async fn select_subtitles(video_id: &str) -> Result<Option<String>> {
    notify("Subtitles", "Fetching subtitle list...", "low");
    let mut subs = get_subtitles(video_id).await?;
    
    if subs.is_empty() {
        notify("Subtitles", "No subtitles found.", "normal");
        return Ok(None);
    }

    subs.insert(0, "🚫 None".to_string());
    let selection = walker_dmenu("Select Subtitles", subs).await?;
    
    if selection.is_empty() || selection.contains("None") {
        return Ok(None);
    }

    let re = Regex::new(r"\((.*?)\)$").unwrap();
    if let Some(caps) = re.captures(&selection) {
        return Ok(Some(caps[1].to_string()));
    }
    Ok(None)
}

async fn process_audio(video_id: &str, mode: &str) -> Result<PathBuf> {
    let cache_dir = expand_path(CACHE_DIR);
    let work_dir = cache_dir.join(format!("proc_{}", video_id));
    fs::create_dir_all(&work_dir)?;

    let audio_path = work_dir.join("input.m4a");
    let chunks_dir = work_dir.join("chunks");
    let out_chunks_dir = work_dir.join("out_chunks");
    let playback_file = work_dir.join("live_audio.pcm");

    if playback_file.exists() { fs::remove_file(&playback_file)?; }
    fs::create_dir_all(&chunks_dir)?;
    fs::create_dir_all(&out_chunks_dir)?;

    notify("Live AI Stream", "Step 1/3: Downloading & Splitting...", "critical");

    if !audio_path.exists() {
        Command::new(expand_path(YT_DLP_BIN))
            .args(["-f", "bestaudio[ext=m4a]/bestaudio", "-o", audio_path.to_str().unwrap(), "--no-playlist", video_id])
            .status()
            .await?;
    }

    // Split into chunks
    Command::new("ffmpeg")
        .args(["-y", "-i", audio_path.to_str().unwrap(), "-f", "segment", "-segment_time", "30", "-c", "copy", chunks_dir.join("chunk_%03d.m4a").to_str().unwrap()])
        .status()
        .await?;

    let mut entries: Vec<_> = fs::read_dir(&chunks_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map_or(false, |ext| ext == "m4a"))
        .collect();
    entries.sort();

    let total = entries.len();
    let mode = mode.to_string();
    let playback_file_clone = playback_file.clone();
    let demucs_bin = expand_path(DEMUCS_BIN);
    let out_chunks_dir_clone = out_chunks_dir.clone();

    tokio::spawn(async move {
        for (i, chunk_path) in entries.iter().enumerate() {
            let chunk_name = chunk_path.file_stem().unwrap().to_str().unwrap();
            let stem_name = if mode == "vocals" { "vocals.wav" } else { "no_vocals.wav" };
            let separated_wav = out_chunks_dir_clone.join("htdemucs").join(chunk_name).join(stem_name);

            // SMART SKIP: Only run Demucs if the output doesn't exist
            if !separated_wav.exists() {
                let _ = Command::new("systemd-run")
                    .args([
                        "--user", "--scope", "-p", "MemoryMax=10G", "-p", "CPUQuota=400%",
                        "-E", "OMP_NUM_THREADS=8", "-E", "MKL_NUM_THREADS=8",
                        "-E", "OPENBLAS_NUM_THREADS=8", "-E", "VECLIB_MAXIMUM_THREADS=8",
                        demucs_bin.to_str().unwrap(), "-n", "htdemucs", "--two-stems=vocals",
                        "--segment", "7", "--shifts", "0", "--overlap", "0.1", "-d", "cpu", "-j", "1",
                        "-o", out_chunks_dir_clone.to_str().unwrap(), chunk_path.to_str().unwrap()
                    ])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .await;
            }

            if separated_wav.exists() {
                let output = Command::new("ffmpeg")
                    .args(["-y", "-i", separated_wav.to_str().unwrap(), "-f", "s16le", "-acodec", "pcm_s16le", "-ar", "44100", "-ac", "2", "-"])
                    .output()
                    .await;

                if let Ok(output) = output {
                    if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(&playback_file_clone) {
                        let _ = f.write_all(&output.stdout);
                        let _ = f.sync_all();
                    }
                }
            }
            notify("Live AI Stream", &format!("Separating: Chunk {}/{}", i + 1, total), "low");
        }
    });

    // Wait for buffer: 2 chunks or 10MB
    let start_time = std::time::Instant::now();
    loop {
        if playback_file.exists() && fs::metadata(&playback_file)?.len() >= 10_000_000 {
            break;
        }
        // Fallback if video is short
        if start_time.elapsed() > Duration::from_secs(240) {
            if playback_file.exists() && fs::metadata(&playback_file)?.len() > 0 { break; }
            return Err(anyhow!("Timeout waiting for audio buffer"));
        }
        sleep(Duration::from_secs(1)).await;
    }

    Ok(playback_file)
}

#[tokio::main]
async fn main() -> Result<()> {
    // One-time old cache cleanup check
    let _ = cleanup_old_cache().await;

    let args = Args::parse();
    
    let query = if !args.query.is_empty() {
        args.query.join(" ")
    } else {
        walker_dmenu("Search YouTube", vec![]).await?
    };

    if query.is_empty() { return Ok(()); }

    notify("Searching", &format!("Searching for: {}", query), "normal");
    let videos = search_youtube(&query).await?;
    if videos.is_empty() {
        notify("Walker YT", "No results found.", "normal");
        return Ok(());
    }

    let display_lines: Vec<String> = videos.iter().map(|v| format!("{} ({})", v.title, v.channel)).collect();
    let selected_str = walker_dmenu("Select Video", display_lines.clone()).await?;
    if selected_str.is_empty() { return Ok(()); }

    let selected_index = display_lines.iter().position(|s| s == &selected_str).ok_or_else(|| anyhow!("Selection mismatch"))?;
    let selected_video = &videos[selected_index];

    let actions = vec![
        "🎬 Watch Video (Auto)".to_string(),
        "⚙️ Watch Video (Select Quality & Subs)".to_string(),
        "🎧 Listen Audio (MPV --no-video)".to_string(),
        "🎤 Keep Vocals (Select Quality & Subs)".to_string(),
        "🎵 Keep Music (Select Quality & Subs)".to_string(),
    ];
    let action_str = walker_dmenu(&format!("Action: {}", selected_video.title), actions).await?;
    if action_str.is_empty() { return Ok(()); }

    let url = format!("https://www.youtube.com/watch?v={}", selected_video.id);
    let mpv_cmd = vec![
        "mpv".to_string(),
        "--title=walker-yt".to_string(),
        format!("--script-opts=ytdl_hook-ytdl_path={}", expand_path(YT_DLP_BIN).to_str().unwrap()),
        "--force-window".to_string(),
        "--cache=yes".to_string(),
        "--cache-pause-wait=5".to_string(),
        "--demuxer-readahead-secs=20".to_string(),
    ];

    let mut video_format = "bestvideo+bestaudio/best".to_string();
    let mut subtitle_code: Option<String> = None;

    if action_str.contains("Select Quality") {
        let qualities = get_video_qualities(&selected_video.id).await?;
        let quality_selection = walker_dmenu("Select Video Quality", qualities).await?;
        if !quality_selection.is_empty() {
            let re = Regex::new(r"(\d+)p(\d+)?").unwrap();
            if let Some(caps) = re.captures(&quality_selection) {
                let h = &caps[1];
                let fps = caps.get(2).map(|m| m.as_str());
                video_format = if let Some(f) = fps {
                    format!("bestvideo[height<={}][fps<={}]", h, f)
                } else {
                    format!("bestvideo[height<={}]", h)
                };
                if action_str.contains("Watch Video") {
                    video_format.push_str("+bestaudio/best");
                }
            }
        }
        
        // Fetch and select subtitles
        subtitle_code = select_subtitles(&selected_video.id).await?;
    }

    if action_str.contains("Watch Video") {
        notify("Playing", &selected_video.title, "normal");
        let mut final_args = mpv_cmd;
        final_args.extend([url, format!("--ytdl-format={}", video_format)]);
        
        if let Some(code) = subtitle_code {
            final_args.extend([
                format!("--ytdl-raw-options=write-subs=,write-auto-sub=,sub-langs=\"{}.*\"", code),
                "--sub-visibility=yes".to_string(),
                "--sub-auto=all".to_string(),
                "--sid=1".to_string(),
            ]);
        }
        
        Command::new("mpv").args(&final_args[1..]).spawn()?;
    } else if action_str.contains("Listen Audio") {
        notify("Playing Audio", &selected_video.title, "normal");
        Command::new("mpv").args(["--no-video", &url]).spawn()?;
    } else if action_str.contains("Keep Vocals") || action_str.contains("Keep Music") {
        let mode = if action_str.contains("Keep Vocals") { "vocals" } else { "music" };
        
        // Cleanup previous sessions
        let _ = Command::new("pkill").args(["-f", "demucs"]).status().await;
        let _ = Command::new("pkill").args(["-f", "mpv.*--title=walker-yt"]).status().await;

        let audio_pcm = process_audio(&selected_video.id, mode).await?;
        
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        
        let audio_pcm_clone = audio_pcm.clone();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                if let Ok(mut f) = fs::File::open(&audio_pcm_clone) {
                    let mut buffer = [0; 16384];
                    loop {
                        let n = f.read(&mut buffer).unwrap_or(0);
                        if n > 0 {
                            if stream.write_all(&buffer[..n]).await.is_err() { break; }
                        } else {
                            sleep(Duration::from_millis(500)).await;
                        }
                    }
                }
            }
        });

        let live_args = vec![
            format!("--audio-file=tcp://127.0.0.1:{}", port),
            "--audio-demuxer=rawaudio".to_string(),
            "--demuxer-rawaudio-rate=44100".to_string(),
            "--demuxer-rawaudio-channels=2".to_string(),
            "--demuxer-rawaudio-format=s16le".to_string(),
            "--cache=yes".to_string(),
            "--cache-secs=3600".to_string(),
            "--aid=1".to_string(),
        ];
        
        if !action_str.contains("Select Quality") {
            video_format = "bestvideo".to_string();
        } else if !video_format.contains("bestvideo") {
            video_format = "bestvideo".to_string();
        }

        let mut final_args = mpv_cmd;
        final_args.extend([url, format!("--ytdl-format={}", video_format)]);
        final_args.extend(live_args);
        
        if let Some(code) = subtitle_code {
            final_args.extend([
                format!("--ytdl-raw-options=write-subs=,write-auto-sub=,sub-langs=\"{}.*\"", code),
                "--sub-visibility=yes".to_string(),
                "--sub-auto=all".to_string(),
                "--sid=1".to_string(),
            ]);
        }
        
        let mut child = Command::new("mpv").args(&final_args[1..]).spawn()?;
        child.wait().await?;
        let _ = Command::new("pkill").args(["-f", "demucs"]).status().await;
    }

    Ok(())
}
