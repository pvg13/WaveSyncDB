use wavesyncdb::{CrudModel, derive::{CrudModel, SyncedCrudModel}};
use wavesyncdb::SyncedModel;

#[derive(CrudModel, SyncedCrudModel, Debug, Default)]
pub struct Task {
    #[primary_key]
    pub id: i32,
    pub title: String,
    pub description: Option<String>,
    pub completed: bool,
    // pub created_at: Option<chrono::NaiveDateTime>,
    // pub updated_at: Option<chrono::NaiveDateTime>,
}
