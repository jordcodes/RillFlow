use clap::{ArgAction, Parser, Subcommand};
use rillflow::projection_runtime::{ProjectionDaemon, ProjectionWorkerConfig};
use rillflow::{SchemaConfig, Store, TenancyMode, TenantSchema};

#[derive(Parser, Debug)]
#[command(name = "rillflow", version, about = "Rillflow CLI")]
struct Cli {
    /// Postgres connection string. Falls back to DATABASE_URL.
    #[arg(long)]
    database_url: Option<String>,

    /// Base schema to manage (default: public)
    #[arg(long, default_value = "public")]
    schema: String,

    /// Additional tenant schemas (repeatable)
    #[arg(long = "tenant-schema", action = ArgAction::Append)]
    tenant_schemas: Vec<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Show planned DDL changes without applying
    SchemaPlan,

    /// Apply DDL changes (create schemas/tables/indexes as needed)
    SchemaSync,

    /// Projection admin commands
    #[command(subcommand)]
    Projections(ProjectionsCmd),
}

#[derive(Subcommand, Debug)]
enum ProjectionsCmd {
    /// List projections and their status
    List,
    /// Show status of a single projection
    Status { name: String },
    /// Pause a projection
    Pause { name: String },
    /// Resume a projection
    Resume { name: String },
    /// Reset checkpoint to a specific sequence
    ResetCheckpoint { name: String, seq: i64 },
    /// Rebuild (reset to 0 and clear DLQ)
    Rebuild { name: String },
    /// Run a single processing tick for one projection (by name) or all registered if omitted
    RunOnce { name: Option<String> },
}

#[tokio::main]
async fn main() -> rillflow::Result<()> {
    let cli = Cli::parse();

    let url = match cli
        .database_url
        .or_else(|| std::env::var("DATABASE_URL").ok())
    {
        Some(u) => u,
        None => {
            eprintln!("error: --database-url or env DATABASE_URL is required");
            std::process::exit(2);
        }
    };

    let tenancy_mode = if cli.tenant_schemas.is_empty() {
        TenancyMode::SingleTenant
    } else {
        TenancyMode::SchemaPerTenant {
            tenants: cli
                .tenant_schemas
                .iter()
                .map(|s| TenantSchema::new(s.trim()))
                .collect(),
        }
    };

    let config = SchemaConfig {
        base_schema: cli.schema,
        tenancy_mode,
    };

    let store = Store::connect(&url).await?;
    let mgr = store.schema();

    match cli.command {
        Commands::SchemaPlan => {
            let plan = mgr.plan(&config).await?;
            print_plan(&plan);
        }
        Commands::SchemaSync => {
            let plan = mgr.sync(&config).await?;
            if plan.is_empty() {
                println!("No changes needed.");
            } else {
                println!("Applied changes:");
                print_plan(&plan);
            }
        }
        Commands::Projections(cmd) => {
            let daemon =
                ProjectionDaemon::new(store.pool().clone(), ProjectionWorkerConfig::default());
            match cmd {
                ProjectionsCmd::List => {
                    let list = daemon.list().await?;
                    for s in list {
                        println!(
                            "{}  last_seq={}  paused={}  leased_by={:?}  lease_until={:?}  backoff_until={:?}  dlq_count={}",
                            s.name,
                            s.last_seq,
                            s.paused,
                            s.leased_by,
                            s.lease_until,
                            s.backoff_until,
                            s.dlq_count
                        );
                    }
                }
                ProjectionsCmd::Status { name } => {
                    let s = daemon.status(&name).await?;
                    println!(
                        "{}  last_seq={}  paused={}  leased_by={:?}  lease_until={:?}  backoff_until={:?}  dlq_count={}",
                        s.name,
                        s.last_seq,
                        s.paused,
                        s.leased_by,
                        s.lease_until,
                        s.backoff_until,
                        s.dlq_count
                    );
                }
                ProjectionsCmd::Pause { name } => {
                    daemon.pause(&name).await?;
                    println!("paused {}", { name });
                }
                ProjectionsCmd::Resume { name } => {
                    daemon.resume(&name).await?;
                    println!("resumed {}", { name });
                }
                ProjectionsCmd::ResetCheckpoint { name, seq } => {
                    daemon.reset_checkpoint(&name, seq).await?;
                    println!("reset {} to {}", name, seq);
                }
                ProjectionsCmd::Rebuild { name } => {
                    daemon.rebuild(&name).await?;
                    println!("rebuild scheduled for {}", name);
                }
                ProjectionsCmd::RunOnce { name } => {
                    if let Some(n) = name {
                        let res = daemon.tick_once(&n).await?;
                        println!("{}: {:?}", n, res);
                    } else {
                        daemon.tick_all_once().await?;
                        println!("tick-all executed");
                    }
                }
            }
        }
    }

    Ok(())
}

fn print_plan(plan: &rillflow::SchemaPlan) {
    if !plan.warnings().is_empty() {
        eprintln!("Warnings ({}):", plan.warnings().len());
        for w in plan.warnings() {
            eprintln!("  - {}", w);
        }
    }

    if plan.actions().is_empty() {
        println!("No pending DDL actions.");
        return;
    }

    println!("DDL actions ({}):", plan.actions().len());
    for (i, action) in plan.actions().iter().enumerate() {
        println!("{}. {}", i + 1, action.description());
        println!("{}\n", action.sql());
    }
}
