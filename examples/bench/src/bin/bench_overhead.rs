//! Bench 1 — local write interception cost.
//!
//! Compares raw SeaORM throughput against WaveSyncDb (single peer, no
//! sync targets) for INSERT / UPDATE / DELETE. The ratio answers "what
//! does adding WaveSyncDB cost on the local write path".

use std::time::Instant;

use bench_common::{task, temp_db_url};
use sea_orm::{
    ActiveModelTrait, ConnectionTrait, Database, DatabaseConnection, EntityTrait, Schema, Set,
    sea_query::SqliteQueryBuilder,
};
use uuid::Uuid;
use wavesyncdb::WaveSyncDbBuilder;

const N: usize = 5_000;

#[tokio::main]
async fn main() {
    let _ = env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Off)
        .try_init();

    println!("bench_overhead — N = {} per operation", N);
    println!();

    let raw = run_raw().await;
    let wsdb = run_wsdb().await;

    print_table(&raw, &wsdb);

    let json = serde_json::json!({
        "bench": "overhead",
        "n": N,
        "raw": {
            "insert_ops_per_sec": raw.insert,
            "update_ops_per_sec": raw.update,
            "delete_ops_per_sec": raw.delete,
        },
        "wavesyncdb": {
            "insert_ops_per_sec": wsdb.insert,
            "update_ops_per_sec": wsdb.update,
            "delete_ops_per_sec": wsdb.delete,
        },
    });
    println!();
    println!("BENCH_RESULT: {}", json);
}

#[derive(Default)]
struct Numbers {
    insert: f64,
    update: f64,
    delete: f64,
}

async fn run_raw() -> Numbers {
    let url = temp_db_url("raw");
    let db: DatabaseConnection = Database::connect(&url).await.unwrap();
    create_tasks_table(&db).await;
    timed_crud(&db).await
}

async fn run_wsdb() -> Numbers {
    let url = temp_db_url("wsdb");
    let db = WaveSyncDbBuilder::new(&url, "bench-overhead")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();
    timed_crud(&db).await
}

async fn create_tasks_table<C: ConnectionTrait>(db: &C) {
    let backend = db.get_database_backend();
    let schema = Schema::new(backend);
    let stmt = schema
        .create_table_from_entity(task::Entity)
        .if_not_exists()
        .to_owned()
        .to_string(SqliteQueryBuilder);
    db.execute_unprepared(&stmt).await.unwrap();
}

async fn timed_crud<C: ConnectionTrait>(db: &C) -> Numbers {
    let ids: Vec<String> = (0..N)
        .map(|_| Uuid::new_v4().simple().to_string())
        .collect();

    // INSERT
    let t = Instant::now();
    for id in &ids {
        task::ActiveModel {
            id: Set(id.clone()),
            title: Set("bench".into()),
            completed: Set(false),
        }
        .insert(db)
        .await
        .unwrap();
    }
    let insert_secs = t.elapsed().as_secs_f64();

    // UPDATE
    let t = Instant::now();
    for id in &ids {
        task::ActiveModel {
            id: Set(id.clone()),
            title: Set("bench-updated".into()),
            completed: Set(true),
        }
        .update(db)
        .await
        .unwrap();
    }
    let update_secs = t.elapsed().as_secs_f64();

    // DELETE
    let t = Instant::now();
    for id in &ids {
        task::Entity::delete_by_id(id.clone())
            .exec(db)
            .await
            .unwrap();
    }
    let delete_secs = t.elapsed().as_secs_f64();

    Numbers {
        insert: N as f64 / insert_secs,
        update: N as f64 / update_secs,
        delete: N as f64 / delete_secs,
    }
}

fn print_table(raw: &Numbers, wsdb: &Numbers) {
    println!(
        "{:<12} {:>14} {:>14} {:>10}",
        "operation", "raw seaorm", "wavesyncdb", "ratio"
    );
    println!("{:-<12} {:->14} {:->14} {:->10}", "", "", "", "");
    print_row("INSERT", raw.insert, wsdb.insert);
    print_row("UPDATE", raw.update, wsdb.update);
    print_row("DELETE", raw.delete, wsdb.delete);
}

fn print_row(op: &str, raw: f64, wsdb: f64) {
    let ratio = if raw > 0.0 { wsdb / raw } else { 0.0 };
    println!(
        "{:<12} {:>10.0}/s {:>10.0}/s {:>9.2}x",
        op, raw, wsdb, ratio
    );
}
