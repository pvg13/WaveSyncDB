use proc_macro::TokenStream;

/// Derive macro that registers a SeaORM entity for auto-discovery by
/// `WaveSyncDb::get_schema_registry`.
///
/// Place this alongside `DeriveEntityModel` on your `Model` struct:
///
/// ```ignore
/// #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, SyncEntity)]
/// #[sea_orm(table_name = "tasks")]
/// pub struct Model { ... }
/// ```
///
/// At link time, the entity's metadata is submitted to an `inventory` collection
/// keyed by the entity's `module_path!()`. When you call
/// `db.get_schema_registry("my_crate")`, it iterates all registered entities
/// whose module path starts with `"my_crate"` and adds them to the schema builder.
///
/// This means entities in different crates are naturally namespaced — pass your
/// crate name (or `module_path!().split("::").next().unwrap()`) to discover only
/// your own entities.
///
/// **Note:** `SyncEntity` must be used alongside `DeriveEntityModel`. It reads
/// the entity's `table_name`, columns, and primary key from SeaORM's generated
/// `Entity`, `Column`, and `PrimaryKey` types.
#[proc_macro_derive(SyncEntity)]
pub fn derive_sync_entity(_input: TokenStream) -> TokenStream {
    quote::quote! {
        wavesyncdb::register_sync_entity! {
            wavesyncdb::SyncEntityInfo {
                module_path: module_path!(),
                schema_fn: |backend| {
                    use sea_orm::{EntityTrait, Iterable, IdenStatic, PrimaryKeyToColumn, Schema};
                    use sea_orm::sea_query::SqliteQueryBuilder;

                    let schema = Schema::new(backend);
                    let create_sql = schema
                        .create_table_from_entity(Entity)
                        .if_not_exists()
                        .to_owned()
                        .to_string(SqliteQueryBuilder);

                    let table_name = Entity.table_name().to_string();
                    let columns: Vec<String> = Column::iter()
                        .map(|c| IdenStatic::as_str(&c).to_string())
                        .collect();
                    let primary_key_column = PrimaryKey::iter()
                        .next()
                        .map(|pk| IdenStatic::as_str(&pk.into_column()).to_string())
                        .unwrap_or_default();

                    (create_sql, wavesyncdb::TableMeta {
                        table_name,
                        primary_key_column,
                        columns,
                    })
                },
            }
        }
    }
    .into()
}
