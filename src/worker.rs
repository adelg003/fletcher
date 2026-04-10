use crate::{
    error::{Error, Result},
    model::{DataProductId, DatasetId, State},
};
use clap::{Args, Parser, Subcommand, ValueEnum};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Instant,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command as TokioCommand,
};
use tracing::info;
use uuid::Uuid;

#[derive(Parser)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<CliCommand>,
}

#[derive(Subcommand)]
pub enum CliCommand {
    Worker(WorkerArgs),
}

#[derive(Args, Clone, Debug)]
pub struct WorkerArgs {
    #[arg(long)]
    pub remote: String,
    #[arg(long)]
    pub dataset_id: DatasetId,
    #[arg(long)]
    pub data_product_id: DataProductId,
    #[arg(long)]
    pub run_id: Uuid,
    #[arg(long)]
    pub script: PathBuf,
    #[arg(long, value_enum)]
    pub runner: WorkerRunner,
    #[arg(long, default_value = "local")]
    pub service: String,
    #[arg(long)]
    pub key: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum WorkerRunner {
    Bash,
    Python,
}

impl WorkerRunner {
    pub const fn as_arg(self) -> &'static str {
        match self {
            WorkerRunner::Bash => "bash",
            WorkerRunner::Python => "python",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LocalWorkerSpec {
    pub runner: WorkerRunner,
    pub script: String,
}

#[derive(Deserialize)]
struct AuthResponse {
    access_token: String,
}

#[derive(Serialize)]
struct WorkerLogParam<'a> {
    run_id: String,
    stream: &'a str,
    message: &'a str,
}

pub fn extract_worker_spec(passthrough: &Option<Value>) -> Result<Option<LocalWorkerSpec>> {
    let Some(value) = passthrough.as_ref() else {
        return Ok(None);
    };

    let Some(worker) = value.get("worker").cloned() else {
        return Ok(None);
    };

    Ok(Some(serde_json::from_value(worker)?))
}

pub async fn run_worker(args: WorkerArgs) -> Result<()> {
    let started_at = Instant::now();
    let client = Client::builder().build()?;
    let token = authenticate(&client, &args).await?;

    update_state(
        &client,
        &args,
        &token,
        State::Running,
        Some(json!({
            "runner": args.runner,
            "script": args.script.display().to_string(),
            "phase": "started",
        })),
    )
    .await?;

    let pyproject_used = has_pyproject(&args.script);
    let spawned_command = rendered_command(&args, pyproject_used);
    info!(
        dataset_id = %args.dataset_id,
        data_product_id = %args.data_product_id,
        run_id = %args.run_id,
        runner = %args.runner.as_arg(),
        script = %args.script.display(),
        remote = %args.remote,
        command_line = %spawned_command,
        "executing worker command"
    );
    let mut command = build_command(&args);
    command.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            update_state(
                &client,
                &args,
                &token,
                State::Failed,
                Some(json!({
                    "runner": args.runner,
                    "script": args.script.display().to_string(),
                    "pyproject": pyproject_used,
                    "reason": err.to_string(),
                })),
            )
            .await?;
            return Err(err.into());
        }
    };
    let stdout = child.stdout.take().ok_or(Error::Unreachable)?;
    let stderr = child.stderr.take().ok_or(Error::Unreachable)?;

    let stdout_task = tokio::spawn(stream_output(
        client.clone(),
        args.clone(),
        token.clone(),
        BufReader::new(stdout),
        "stdout",
    ));
    let stderr_task = tokio::spawn(stream_output(
        client.clone(),
        args.clone(),
        token.clone(),
        BufReader::new(stderr),
        "stderr",
    ));

    let status = child.wait().await?;
    let stdout_result = stdout_task.await.map_err(Error::Join)?;
    let stderr_result = stderr_task.await.map_err(Error::Join)?;

    if let Err(err) = stdout_result.as_ref().or(stderr_result.as_ref()) {
        update_state(
            &client,
            &args,
            &token,
            State::Failed,
            Some(json!({
                "runner": args.runner,
                "script": args.script.display().to_string(),
                "pyproject": pyproject_used,
                "duration_ms": started_at.elapsed().as_millis(),
                "reason": err.to_string(),
            })),
        )
        .await?;
    }

    stdout_result?;
    stderr_result?;

    let exit_code = status.code().unwrap_or(-1);
    let final_state = if status.success() {
        State::Success
    } else {
        State::Failed
    };

    update_state(
        &client,
        &args,
        &token,
        final_state,
        Some(json!({
            "runner": args.runner,
            "script": args.script.display().to_string(),
            "pyproject": pyproject_used,
            "duration_ms": started_at.elapsed().as_millis(),
            "exit_code": exit_code,
        })),
    )
    .await?;

    if status.success() {
        Ok(())
    } else {
        Err(Error::WorkerExit(exit_code))
    }
}

fn build_command(args: &WorkerArgs) -> TokioCommand {
    match args.runner {
        WorkerRunner::Bash => {
            let mut command = TokioCommand::new("bash");
            command.arg(&args.script);
            command
        }
        WorkerRunner::Python => {
            if has_pyproject(&args.script) {
                let script_dir = args
                    .script
                    .parent()
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| PathBuf::from("."));
                let script_name = args
                    .script
                    .file_name()
                    .map(|name| name.to_owned())
                    .unwrap_or_else(|| args.script.as_os_str().to_owned());
                let mut command = TokioCommand::new("uv");
                command.current_dir(script_dir);
                command.args(["run", "python"]);
                command.arg(script_name);
                command
            } else {
                let mut command = TokioCommand::new("python3");
                command.arg(&args.script);
                command
            }
        }
    }
}

fn has_pyproject(script: &Path) -> bool {
    script
        .parent()
        .map(|dir| dir.join("pyproject.toml").exists())
        .unwrap_or(false)
}

fn rendered_command(args: &WorkerArgs, pyproject_used: bool) -> String {
    match args.runner {
        WorkerRunner::Bash => format!("bash {}", args.script.display()),
        WorkerRunner::Python if pyproject_used => {
            let script_dir = args
                .script
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from("."));
            let script_name = args
                .script
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| args.script.display().to_string());
            format!(
                "cd {} && uv run python {}",
                script_dir.display(),
                script_name
            )
        }
        WorkerRunner::Python => format!("python3 {}", args.script.display()),
    }
}

async fn stream_output<R>(
    client: Client,
    args: WorkerArgs,
    token: String,
    reader: BufReader<R>,
    stream: &'static str,
) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await? {
        post_log(&client, &args, &token, stream, &line).await?;
    }
    Ok(())
}

async fn authenticate(client: &Client, args: &WorkerArgs) -> Result<String> {
    let key = args
        .key
        .clone()
        .or_else(|| std::env::var("FLETCHER_WORKER_KEY").ok())
        .ok_or(Error::Unreachable)?;

    let response = client
        .post(api_url(&args.remote, "authenticate"))
        .json(&json!({
            "service": args.service,
            "key": key,
        }))
        .send()
        .await?
        .error_for_status()?
        .json::<AuthResponse>()
        .await?;

    Ok(response.access_token)
}

async fn update_state(
    client: &Client,
    args: &WorkerArgs,
    token: &str,
    state: State,
    passback: Option<Value>,
) -> Result<()> {
    client
        .put(api_url(
            &args.remote,
            &format!("data_product/{}/update", args.dataset_id),
        ))
        .bearer_auth(token)
        .json(&vec![json!({
            "id": args.data_product_id.to_string(),
            "state": state.to_string(),
            "run_id": args.run_id.to_string(),
            "link": args.script.display().to_string(),
            "passback": passback,
        })])
        .send()
        .await?
        .error_for_status()?;

    Ok(())
}

async fn post_log(
    client: &Client,
    args: &WorkerArgs,
    token: &str,
    stream: &str,
    message: &str,
) -> Result<()> {
    client
        .post(api_url(
            &args.remote,
            &format!(
                "data_product/{}/{}/log",
                args.dataset_id, args.data_product_id
            ),
        ))
        .bearer_auth(token)
        .json(&WorkerLogParam {
            run_id: args.run_id.to_string(),
            stream,
            message,
        })
        .send()
        .await?
        .error_for_status()?;

    Ok(())
}

fn api_url(remote: &str, path: &str) -> String {
    format!("{}/api/{}", remote.trim_end_matches('/'), path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_extract_worker_spec() {
        let passthrough = Some(json!({
            "path": "ignored",
            "worker": {
                "runner": "bash",
                "script": "./scripts/job.sh"
            }
        }));

        let spec = extract_worker_spec(&passthrough).unwrap().unwrap();
        assert_eq!(spec.runner, WorkerRunner::Bash);
        assert_eq!(spec.script, "./scripts/job.sh");
    }
}
