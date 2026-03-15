mod entity;

use entity::task;
use sea_orm::{ActiveModelTrait, EntityTrait, Set};
use uuid::Uuid;
use wavesyncdb::dioxus::{
    use_synced_table, use_wavesync_init, use_wavesync_opt, use_wavesync_provider_lazy,
};

use dioxus::prelude::*;

const STYLE: &str = r#"
    body { font-family: sans-serif; max-width: 600px; margin: 40px auto; padding: 0 20px; }
    h1 { color: #333; }
    .picker { text-align: center; margin-top: 80px; }
    .picker input { padding: 8px; font-size: 14px; border: 1px solid #ccc; border-radius: 4px; width: 300px; }
    .picker button { margin-top: 12px; padding: 10px 24px; background: #4a90d9; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 14px; }
    .picker .error { color: #e74c3c; margin-top: 8px; }
    .add-form { display: flex; gap: 8px; margin-bottom: 20px; }
    .add-form input { flex: 1; padding: 8px; font-size: 14px; border: 1px solid #ccc; border-radius: 4px; }
    .add-form button { padding: 8px 16px; background: #4a90d9; color: white; border: none; border-radius: 4px; cursor: pointer; }
    .task-list { list-style: none; padding: 0; }
    .task-item { display: flex; align-items: center; gap: 8px; padding: 8px 0; border-bottom: 1px solid #eee; }
    .task-item label { flex: 1; cursor: pointer; }
    .task-item label.completed { text-decoration: line-through; color: #999; }
    .task-item button { background: #e74c3c; color: white; border: none; border-radius: 4px; padding: 4px 10px; cursor: pointer; }
    .empty { color: #999; font-style: italic; }
    .loading { text-align: center; margin-top: 40px; color: #999; }
"#;

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .filter_module("libp2p_gossipsub", log::LevelFilter::Warn)
        .filter_module("libp2p_autonat", log::LevelFilter::Warn)
        .filter_module("libp2p_mdns", log::LevelFilter::Warn)
        .filter_module("libp2p_swarm", log::LevelFilter::Warn)
        .init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");
    let _guard = rt.enter();

    dioxus::launch(App);
}

#[allow(non_snake_case)]
fn App() -> Element {
    use_wavesync_provider_lazy();
    wavesyncdb::dioxus::use_wavesync_generation();
    let db = use_wavesync_opt();

    rsx! {
        style { {STYLE} }
        match db() {
            Some(_) => rsx! {
                h1 { "WaveSyncDB Task Manager" }
                AddTaskForm {}
                TaskList {}
            },
            None => rsx! {
                DatabasePicker {}
            },
        }
    }
}

#[component]
fn DatabasePicker() -> Element {
    let init = use_wavesync_init();
    let mut url = use_signal(|| "sqlite:./tasks.db?mode=rwc".to_string());
    let mut error: Signal<Option<String>> = use_signal(|| None);
    let mut loading = use_signal(|| false);

    let on_submit = move |evt: FormEvent| {
        evt.prevent_default();
        let db_url = url.read().clone();
        if db_url.is_empty() {
            return;
        }
        loading.set(true);
        error.set(None);
        spawn(async move {
            let result = init
                .call(&db_url, "dioxus-dynamic-tasks", |db| async move {
                    db.get_schema_registry(module_path!().split("::").next().unwrap())
                        .sync()
                        .await?;
                    Ok(())
                })
                .await;

            if let Err(e) = result {
                error.set(Some(format!("Failed to open database: {e}")));
                loading.set(false);
            }
        });
    };

    rsx! {
        div { class: "picker",
            h1 { "WaveSyncDB Dynamic Example" }
            p { "Enter a SQLite database URL to get started." }
            form { onsubmit: on_submit,
                input {
                    r#type: "text",
                    placeholder: "sqlite:./my.db?mode=rwc",
                    value: "{url}",
                    oninput: move |evt| url.set(evt.value()),
                    disabled: loading(),
                }
                br {}
                button {
                    r#type: "submit",
                    disabled: loading(),
                    if loading() { "Connecting..." } else { "Open Database" }
                }
            }
            if let Some(err) = error() {
                p { class: "error", "{err}" }
            }
        }
    }
}

#[component]
fn AddTaskForm() -> Element {
    let db = use_wavesync_opt()().unwrap();
    let mut input = use_signal(String::new);

    let on_submit = move |evt: FormEvent| {
        evt.prevent_default();
        let title = input.read().trim().to_string();
        if title.is_empty() {
            return;
        }
        input.set(String::new());
        let db = db.clone();
        spawn(async move {
            let new_task = task::ActiveModel {
                id: Set(Uuid::new_v4().to_string()),
                title: Set(title),
                completed: Set(false),
                ..Default::default()
            };
            if let Err(e) = new_task.insert(&db).await {
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
    let db = use_wavesync_opt()().unwrap();
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
    let db = use_wavesync_opt()().unwrap();
    let id = task.id.clone();
    let completed = task.completed;

    let toggle_id = id.clone();
    let db2 = db.clone();
    let toggle = move |_| {
        let id = toggle_id.clone();
        let db = db2.clone();
        spawn(async move {
            let active = task::ActiveModel {
                id: Set(id),
                completed: Set(!completed),
                ..Default::default()
            };
            if let Err(e) = active.update(&db).await {
                log::error!("Failed to toggle task: {}", e);
            }
        });
    };

    let delete_id = id.clone();
    let delete = move |_| {
        let id = delete_id.clone();
        let db = db.clone();
        spawn(async move {
            if let Err(e) = task::Entity::delete_by_id(id).exec(&db).await {
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
