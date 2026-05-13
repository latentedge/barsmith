use anyhow::Result;
use barsmith_rs::protocol::{
    ResearchProtocol, ResearchProtocolDraft, ResearchWindow, load_json, parse_optional_date,
    write_json_pretty,
};

use crate::cli::{ProtocolCommand, ProtocolInitArgs, ProtocolValidateArgs};

pub fn run(command: ProtocolCommand) -> Result<()> {
    match command {
        ProtocolCommand::Init(args) => init(*args),
        ProtocolCommand::Validate(args) => validate(args),
        ProtocolCommand::Explain(args) => explain(args),
    }
}

fn init(args: ProtocolInitArgs) -> Result<()> {
    let protocol = ResearchProtocol::from_draft(ResearchProtocolDraft {
        dataset_id: args.dataset_id,
        target: normalize_target(&args.target),
        direction: args.direction,
        engine: args.engine,
        discovery: ResearchWindow::new(
            parse_optional_date(args.discovery_start.as_deref(), "--discovery-start")?,
            parse_optional_date(args.discovery_end.as_deref(), "--discovery-end")?,
        )?,
        validation: ResearchWindow::new(
            parse_optional_date(args.validation_start.as_deref(), "--validation-start")?,
            parse_optional_date(args.validation_end.as_deref(), "--validation-end")?,
        )?,
        lockbox: ResearchWindow::new(
            parse_optional_date(args.lockbox_start.as_deref(), "--lockbox-start")?,
            parse_optional_date(args.lockbox_end.as_deref(), "--lockbox-end")?,
        )?,
        candidate_top_k: args.candidate_top_k,
    });
    let hash = protocol.hash()?;
    write_json_pretty(&args.output, &protocol)?;
    println!("Research protocol written: {}", args.output.display());
    println!("Protocol SHA-256: {hash}");
    Ok(())
}

fn validate(args: ProtocolValidateArgs) -> Result<()> {
    let protocol: ResearchProtocol = load_json(&args.protocol)?;
    println!(
        "Research protocol is valid JSON: {}",
        args.protocol.display()
    );
    println!("Protocol SHA-256: {}", protocol.hash()?);
    Ok(())
}

fn explain(args: ProtocolValidateArgs) -> Result<()> {
    let protocol: ResearchProtocol = load_json(&args.protocol)?;
    println!("Research protocol: {}", args.protocol.display());
    println!("Protocol SHA-256: {}", protocol.hash()?);
    println!("Dataset: {}", protocol.dataset_id);
    println!("Target: {}", protocol.target);
    if let Some(direction) = protocol.direction.as_deref() {
        println!("Direction: {direction}");
    }
    if let Some(engine) = protocol.engine.as_deref() {
        println!("Engine: {engine}");
    }
    println!(
        "Discovery: {} -> {}",
        fmt_date(protocol.discovery.start),
        fmt_date(protocol.discovery.end)
    );
    println!(
        "Validation: {} -> {}",
        fmt_date(protocol.validation.start),
        fmt_date(protocol.validation.end)
    );
    println!(
        "Lockbox: {} -> {}",
        fmt_date(protocol.lockbox.start),
        fmt_date(protocol.lockbox.end)
    );
    println!(
        "Candidate cap: {}",
        protocol
            .candidate_top_k
            .map(|value| value.to_string())
            .unwrap_or_else(|| "not specified".to_string())
    );
    println!("Strict: {}", protocol.strict);
    Ok(())
}

fn fmt_date(date: Option<chrono::NaiveDate>) -> String {
    date.map(|value| value.to_string())
        .unwrap_or_else(|| "open".to_string())
}

fn normalize_target(target: &str) -> String {
    if target == "atr_stop" {
        "2x_atr_tp_atr_stop".to_string()
    } else {
        target.to_string()
    }
}
