mod entity;

use entity::task;
use sea_orm::{ActiveModelTrait, EntityTrait, Set};
use uuid::Uuid;
use wavesyncdb_dioxus::{use_db, use_synced_row, use_synced_table};

use dioxus::prelude::*;

const STYLE: &str = r#"
    body { font-family: sans-serif; max-width: 600px; margin: 40px auto; padding: 0 20px; }
    h1 { color: #333; }
    .add-form { display: flex; gap: 8px; margin-bottom: 20px; }
    .add-form input { flex: 1; padding: 8px; font-size: 14px; border: 1px solid #ccc; border-radius: 4px; }
    .add-form button { padding: 8px 16px; background: #4a90d9; color: white; border: none; border-radius: 4px; cursor: pointer; }
    .task-list { list-style: none; padding: 0; }
    .task-item { display: flex; align-items: center; gap: 8px; padding: 8px 0; border-bottom: 1px solid #eee; }
    .task-item label { flex: 1; cursor: pointer; }
    .task-item label.completed { text-decoration: line-through; color: #999; }
    .task-item button { background: #e74c3c; color: white; border: none; border-radius: 4px; padding: 4px 10px; cursor: pointer; }
    .empty { color: #999; font-style: italic; }
"#;

fn main() {
    env_logger::init();
    wavesyncdb_dioxus::launch("sqlite::memory:", "dioxus-tasks", |db| async move {
        db.get_schema_registry(module_path!().split("::").next().unwrap())
            .sync()
            .await?;
        Ok(())
    }, App);
}

#[allow(non_snake_case)]
fn App() -> Element {
    rsx! {
        style { {STYLE} }
        h1 { "WaveSyncDB Task Manager" }
        AddTaskForm {}
        TaskList {}
    }
}

#[component]
fn AddTaskForm() -> Element {
    let db = use_db();
    let mut input = use_signal(String::new);

    let on_submit = move |evt: FormEvent| {
        evt.prevent_default();
        let title = input.read().trim().to_string();
        if title.is_empty() {
            return;
        }
        input.set(String::new());
        spawn(async move {
            let new_task = task::ActiveModel {
                id: Set(Uuid::new_v4().to_string()),
                title: Set(title),
                completed: Set(false),
                ..Default::default()
            };
            if let Err(e) = new_task.insert(db).await {
                log::error!("Failed to insert task: {}", e);
            }
        });
    };

    rsx! {
        form { class: "add-form", onsubmit: on_submit,
            input {
                r#type: "text",
                placeholder: "What needs to be done?",
                value: "{input}",
                oninput: move |evt| input.set(evt.value()),
            }
            button { r#type: "submit", "Add" }
        }
    }
}

#[component]
fn TaskList() -> Element {
    let db = use_db();
    let tasks = use_synced_table::<task::Entity>(db);

    rsx! {
        ul { class: "task-list",
            if tasks.read().is_empty() {
                li { class: "empty", "No tasks yet. Add one above!" }
            }
            for t in tasks.read().iter() {
                TaskItem { key: "{t.id}", task: t.clone() }
            }
        }
    }
}

#[component]
fn TaskItem(task: task::Model) -> Element {
    let db = use_db();
    let id = task.id.clone();
    let completed = task.completed;

    let toggle_id = id.clone();
    let toggle = move |_| {
        let id = toggle_id.clone();
        spawn(async move {
            let active = task::ActiveModel {
                id: Set(id),
                completed: Set(!completed),
                ..Default::default()
            };
            if let Err(e) = active.update(db).await {
                log::error!("Failed to toggle task: {}", e);
            }
        });
    };

    let delete_id = id.clone();
    let delete = move |_| {
        let id = delete_id.clone();
        spawn(async move {
            if let Err(e) = task::Entity::delete_by_id(id).exec(db).await {
                log::error!("Failed to delete task: {}", e);
            }
        });
    };

    rsx! {
        li { class: "task-item",
            input {
                r#type: "checkbox",
                checked: task.completed,
                onchange: toggle,
            }
            label {
                class: if task.completed { "completed" } else { "" },
                "{task.title}"
            }
            button { onclick: delete, "Delete" }
        }
    }
}
