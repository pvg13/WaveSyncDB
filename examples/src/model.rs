use diesel::prelude::*;

#[derive(Queryable, Insertable, Debug)]
#[diesel(table_name = crate::schema::tasks)]
pub struct Task {
    pub id: Option<i32>,
    pub title: String,
    pub description: Option<String>,
    pub completed: bool,
    pub created_at: Option<chrono::NaiveDateTime>,
    pub updated_at: Option<chrono::NaiveDateTime>,
}