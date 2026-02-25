use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait, Set};
use uuid::Uuid;
use wavesyncdb::WaveSyncDbBuilder;

// Define a Task entity using standard SeaORM
mod task {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "tasks")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: String,
        pub title: String,
        pub completed: bool,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

#[tokio::test]
async fn test_wavesyncdb_basic_crud() {
    // Build WaveSyncDb with in-memory SQLite
    let db = WaveSyncDbBuilder::new("sqlite::memory:", "test-crud")
        .build()
        .await
        .expect("Failed to create WaveSyncDb");

    // Create table and register for sync
    db.schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema");

    // INSERT
    let new_task = task::ActiveModel {
        id: Set(Uuid::new_v4().to_string()),
        title: Set("Buy milk".into()),
        completed: Set(false),
        ..Default::default()
    };
    let inserted = new_task.insert(&db).await.expect("Failed to insert");
    assert_eq!(inserted.title, "Buy milk");
    assert!(!inserted.completed);
    assert!(!inserted.id.is_empty());

    // SELECT all
    let all = task::Entity::find()
        .all(&db)
        .await
        .expect("Failed to find all");
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].title, "Buy milk");

    // SELECT by id
    let found = task::Entity::find_by_id(inserted.id.clone())
        .one(&db)
        .await
        .expect("Failed to find by id");
    assert!(found.is_some());
    assert_eq!(found.unwrap().title, "Buy milk");

    // UPDATE
    let mut active: task::ActiveModel = inserted.into();
    active.title = Set("Buy bread".into());
    active.completed = Set(true);
    let updated = active.update(&db).await.expect("Failed to update");
    assert_eq!(updated.title, "Buy bread");
    assert!(updated.completed);

    // DELETE
    let delete_result = task::Entity::delete_by_id(updated.id)
        .exec(&db)
        .await
        .expect("Failed to delete");
    assert_eq!(delete_result.rows_affected, 1);

    // Verify deleted
    let all_after_delete = task::Entity::find()
        .all(&db)
        .await
        .expect("Failed to find after delete");
    assert!(all_after_delete.is_empty());
}

#[tokio::test]
async fn test_wavesyncdb_sync_log_created() {
    let db = WaveSyncDbBuilder::new("sqlite::memory:", "test-sync-log")
        .build()
        .await
        .expect("Failed to create WaveSyncDb");

    // Verify that the _wavesync_log table was created
    let result = db
        .execute_unprepared("SELECT count(*) FROM _wavesync_log")
        .await;
    assert!(result.is_ok(), "sync log table should exist");
}

#[tokio::test]
async fn test_wavesyncdb_change_notifications() {
    let db = WaveSyncDbBuilder::new("sqlite::memory:", "test-notifications")
        .build()
        .await
        .expect("Failed to create WaveSyncDb");

    // Create table and register for sync
    db.schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema");

    // Subscribe to changes before writing
    let mut rx = db.change_rx();

    // Insert a task
    let new_task = task::ActiveModel {
        id: Set(Uuid::new_v4().to_string()),
        title: Set("Test notification".into()),
        completed: Set(false),
        ..Default::default()
    };
    let _ = new_task.insert(&db).await.expect("Failed to insert");

    // Give the async dispatch a moment to fire
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Check that we received a change notification
    let notification = rx.try_recv();
    assert!(
        notification.is_ok(),
        "Should have received a change notification"
    );
    let notif = notification.unwrap();
    assert_eq!(notif.table, "tasks");
}
