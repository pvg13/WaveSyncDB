use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input, spanned::Spanned};

/// Derive macro that emits the per-target glue needed to plug a Rust
/// struct into WaveSyncDB's sync engine.
///
/// **On native targets (`cfg(not(target_arch = "wasm32"))`):**
///
/// 1. Registers a SeaORM entity for auto-discovery by
///    `WaveSyncDb::get_schema_registry`.
/// 2. Emits `impl SyncedModel` so the Dioxus reactive hooks can apply
///    per-column changes from `ChangeNotification` in place — no DB
///    round-trip on the receive path.
///
/// **On wasm32:**
///
/// Emits `impl BrowserEntity` so the same struct works with
/// `WebSyncClient::submit` and the web `use_synced_table` hook.
/// Field values round-trip through `serde_json::Value`, so each non-PK
/// field must implement `Serialize + DeserializeOwned + Default`
/// (primitives, `String`, `Option<T>`, `Vec<u8>`, chrono dates — all
/// satisfy this out of the box).
///
/// ## Patterns
///
/// **Wasm-only entity** (e.g. for a browser-only demo):
///
/// ```ignore
/// #[derive(Clone, Debug, Default, SyncEntity)]
/// pub struct Task {
///     #[sea_orm(primary_key)]
///     pub id: String,
///     pub title: String,
///     pub done: bool,
/// }
/// ```
///
/// **Shared across native and wasm** (one source of truth — the
/// SeaORM derives are conditional, but `SyncEntity` is unconditional and
/// claims the `#[sea_orm(...)]` helper attribute on both targets):
///
/// ```ignore
/// #[derive(Clone, Debug, Default, SyncEntity)]
/// #[cfg_attr(not(target_arch = "wasm32"), derive(sea_orm::DeriveEntityModel))]
/// #[cfg_attr(not(target_arch = "wasm32"), sea_orm(table_name = "tasks"))]
/// pub struct Model {
///     #[sea_orm(primary_key)]
///     pub id: String,
///     pub title: String,
///     pub done: bool,
/// }
/// ```
#[proc_macro_derive(SyncEntity, attributes(sea_orm))]
pub fn derive_sync_entity(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let meta = match collect_field_meta(&input) {
        Ok(m) => m,
        Err(e) => return e.to_compile_error().into(),
    };

    let table_name = match parse_table_name(&input) {
        Ok(name) => name,
        Err(e) => return e.to_compile_error().into(),
    };

    let inventory_block = quote! {
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
                        delete_policy: wavesyncdb::DeletePolicy::DeleteWins,
                    })
                },
            }
        }
    };

    let synced_model_body = build_synced_model_body(&meta);
    let browser_entity_body = build_browser_entity_body(&meta);

    let model_ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    quote! {
        #[cfg(not(target_arch = "wasm32"))]
        #inventory_block

        #[cfg(not(target_arch = "wasm32"))]
        #[automatically_derived]
        impl #impl_generics ::wavesyncdb::SyncedModel for #model_ident #ty_generics #where_clause {
            #synced_model_body
        }

        #[cfg(target_arch = "wasm32")]
        #[automatically_derived]
        impl #impl_generics ::wavesyncdb::BrowserEntity for #model_ident #ty_generics #where_clause {
            #browser_entity_body
        }

        // The cross-target metadata trait that lets the unified
        // `wavesyncdb::dioxus::use_synced_table` hook work without
        // cfg gating at the UI call site. On native it carries the
        // SeaORM Entity + ActiveModel associations; on wasm it carries
        // only the table name (the column round-trip lives in
        // `BrowserEntity`). The sibling identifiers `Entity` and
        // `ActiveModel` referenced below come from `DeriveEntityModel`
        // (or any compatible derive) in the same module as the user's
        // struct.
        #[cfg(not(target_arch = "wasm32"))]
        #[automatically_derived]
        impl #impl_generics ::wavesyncdb::SyncedTableEntity for #model_ident #ty_generics #where_clause {
            type Entity = Entity;
            type ActiveModel = ActiveModel;
            fn table_name() -> &'static str { #table_name }
        }

        #[cfg(target_arch = "wasm32")]
        #[automatically_derived]
        impl #impl_generics ::wavesyncdb::SyncedTableEntity for #model_ident #ty_generics #where_clause {
            fn table_name() -> &'static str { #table_name }
        }
    }
    .into()
}

/// Parse the table name from the struct-level `#[sea_orm(table_name = "...")]`
/// attribute. Required on both native and wasm32 targets — on wasm there's
/// no `DeriveEntityModel` to consume it, but `SyncEntity` claims it via
/// `attributes(sea_orm)` so the compiler accepts it and the macro reads it
/// to populate `SyncedTableEntity::table_name()`.
fn parse_table_name(input: &DeriveInput) -> syn::Result<String> {
    for attr in &input.attrs {
        if !attr.path().is_ident("sea_orm") {
            continue;
        }
        let mut table_name: Option<String> = None;
        let mut parse_err: Option<syn::Error> = None;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("table_name") {
                let value = meta.value()?;
                let lit: syn::LitStr = value.parse()?;
                table_name = Some(lit.value());
            } else if meta.input.peek(syn::Token![=]) {
                // Skip past any `= ...` value for keys we don't care about
                // so the parser doesn't error on them.
                let _: syn::Token![=] = meta.input.parse()?;
                let _: proc_macro2::TokenStream = meta.input.parse()?;
            }
            Ok(())
        })
        .map_err(|e| parse_err = Some(e));
        if let Some(e) = parse_err {
            return Err(e);
        }
        if let Some(name) = table_name {
            return Ok(name);
        }
    }
    Err(syn::Error::new(
        input.span(),
        "SyncEntity requires `#[sea_orm(table_name = \"...\")]` at the struct level — needed to populate `SyncedTableEntity::table_name()`",
    ))
}

struct FieldMeta {
    field_idents: Vec<syn::Ident>,
    field_types: Vec<syn::Type>,
    field_lits: Vec<String>,
    pk_ident: syn::Ident,
    pk_type: syn::Type,
}

/// Walks the struct's named fields and identifies the field marked
/// `#[sea_orm(primary_key)]`. Shared between the native and wasm
/// codegen paths.
fn collect_field_meta(input: &DeriveInput) -> syn::Result<FieldMeta> {
    let fields = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new(
                    s.fields.span(),
                    "SyncEntity requires a struct with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new(
                input.span(),
                "SyncEntity can only be derived on structs",
            ));
        }
    };

    let mut field_idents = Vec::with_capacity(fields.len());
    let mut field_types = Vec::with_capacity(fields.len());
    let mut field_lits = Vec::with_capacity(fields.len());
    let mut pk_field: Option<(syn::Ident, syn::Type)> = None;

    for field in fields.iter() {
        let ident = field
            .ident
            .clone()
            .ok_or_else(|| syn::Error::new(field.span(), "expected named field"))?;
        let ty = field.ty.clone();
        field_lits.push(ident.to_string());

        if has_primary_key_attr(field) {
            pk_field = Some((ident.clone(), ty.clone()));
        }
        field_idents.push(ident);
        field_types.push(ty);
    }

    let (pk_ident, pk_type) = pk_field.ok_or_else(|| {
        syn::Error::new(
            input.span(),
            "SyncEntity requires exactly one field marked #[sea_orm(primary_key)]",
        )
    })?;

    Ok(FieldMeta {
        field_idents,
        field_types,
        field_lits,
        pk_ident,
        pk_type,
    })
}

/// Generates the three `SyncedModel` trait methods used by the native
/// Dioxus reactive hooks to apply per-column changes in place.
fn build_synced_model_body(meta: &FieldMeta) -> proc_macro2::TokenStream {
    let FieldMeta {
        field_idents,
        field_types,
        field_lits,
        pk_ident,
        pk_type,
    } = meta;

    // Local binding names used inside `wavesync_from_changes` to avoid
    // shadowing the struct field name when constructing the final value.
    let local_idents: Vec<syn::Ident> = field_idents
        .iter()
        .map(|i| syn::Ident::new(&format!("__ws_{}", i), i.span()))
        .collect();
    let pk_local = local_idents
        .iter()
        .zip(field_idents.iter())
        .find(|(_, fi)| *fi == pk_ident)
        .map(|(li, _)| li.clone())
        .expect("pk ident must be in field list");

    quote! {
        fn wavesync_apply_change(&mut self, column: &str, value: &::wavesyncdb::serde_json::Value) {
            match column {
                #(
                    #field_lits => {
                        if let Ok(__v) = ::wavesyncdb::serde_json::from_value::<#field_types>(value.clone()) {
                            self.#field_idents = __v;
                        }
                    }
                )*
                _ => {}
            }
        }

        fn wavesync_from_changes(
            _pk_column: &str,
            pk_value: &str,
            changes: &[(::std::string::String, ::wavesyncdb::serde_json::Value)],
        ) -> ::std::option::Option<Self> {
            #(
                let mut #local_idents: ::std::option::Option<#field_types> = ::std::option::Option::None;
            )*

            for (__c, __v) in changes {
                match __c.as_str() {
                    #(
                        #field_lits => {
                            #local_idents = ::wavesyncdb::serde_json::from_value::<#field_types>(__v.clone()).ok();
                        }
                    )*
                    _ => {}
                }
            }

            // PK fallback: if the pk wasn't in `changes`, try parsing
            // `pk_value` directly. Strings deserialize cleanly from a
            // JSON string; numeric / Uuid PKs work via from_str on the
            // raw token (e.g. "42" → 42, "550e8400-..." → Uuid).
            if #pk_local.is_none() {
                #pk_local = ::wavesyncdb::serde_json::from_value::<#pk_type>(
                    ::wavesyncdb::serde_json::Value::String(pk_value.to_string())
                ).ok();
                if #pk_local.is_none() {
                    #pk_local = ::wavesyncdb::serde_json::from_str::<#pk_type>(pk_value).ok();
                }
            }

            ::std::option::Option::Some(Self {
                #( #field_idents: #local_idents? ),*
            })
        }

        fn wavesync_pk_string(&self) -> ::std::string::String {
            ::std::format!("{}", self.#pk_ident)
        }
    }
}

/// Generates `impl BrowserEntity` — bidirectional mapping between a
/// Rust struct and the engine's column-bag wire shape. Each non-PK field
/// is serialized via `serde_json::to_value` on submit and deserialized
/// via `serde_json::from_value` on receive; missing columns fall back to
/// `Default::default()` so the trait method can return `Self` directly
/// (the trait has no `Option<Self>` return path).
fn build_browser_entity_body(meta: &FieldMeta) -> proc_macro2::TokenStream {
    let pk_ident = &meta.pk_ident;
    let pk_type = &meta.pk_type;

    // Partition into PK and non-PK fields. `to_columns` deliberately
    // omits the PK — the trait's contract is that callers pass it
    // separately via `pk()`.
    let mut non_pk_idents = Vec::new();
    let mut non_pk_types = Vec::new();
    let mut non_pk_lits = Vec::new();
    for ((id, ty), lit) in meta
        .field_idents
        .iter()
        .zip(meta.field_types.iter())
        .zip(meta.field_lits.iter())
    {
        if id != pk_ident {
            non_pk_idents.push(id.clone());
            non_pk_types.push(ty.clone());
            non_pk_lits.push(lit.clone());
        }
    }

    quote! {
        fn from_columns(
            pk: &str,
            cols: &::std::collections::HashMap<::std::string::String, ::wavesyncdb::serde_json::Value>,
        ) -> Self {
            // PK parsing chain mirrors the native `wavesync_from_changes`
            // path: try `from_value(Value::String(...))` first (covers
            // String and Uuid), then fall back to `from_str` (covers
            // numeric PKs where serde_json doesn't auto-convert from a
            // JSON string).
            let __pk_val: #pk_type = ::wavesyncdb::serde_json::from_value::<#pk_type>(
                ::wavesyncdb::serde_json::Value::String(pk.to_string()),
            )
            .ok()
            .or_else(|| ::wavesyncdb::serde_json::from_str::<#pk_type>(pk).ok())
            .unwrap_or_default();

            Self {
                #pk_ident: __pk_val,
                #(
                    #non_pk_idents: cols
                        .get(#non_pk_lits)
                        .and_then(|__v| ::wavesyncdb::serde_json::from_value::<#non_pk_types>(__v.clone()).ok())
                        .unwrap_or_default(),
                )*
            }
        }

        fn to_columns(&self) -> ::std::vec::Vec<(::std::string::String, ::wavesyncdb::serde_json::Value)> {
            let mut __out: ::std::vec::Vec<(::std::string::String, ::wavesyncdb::serde_json::Value)>
                = ::std::vec::Vec::new();
            #(
                __out.push((
                    ::std::string::ToString::to_string(#non_pk_lits),
                    ::wavesyncdb::serde_json::to_value(&self.#non_pk_idents)
                        .unwrap_or(::wavesyncdb::serde_json::Value::Null),
                ));
            )*
            __out
        }

        fn pk(&self) -> ::std::string::String {
            ::std::format!("{}", self.#pk_ident)
        }
    }
}

/// True if the field carries `#[sea_orm(primary_key)]` (with or without
/// other inner tokens — `#[sea_orm(primary_key, auto_increment = false)]`
/// counts).
fn has_primary_key_attr(field: &syn::Field) -> bool {
    field.attrs.iter().any(|attr| {
        if !attr.path().is_ident("sea_orm") {
            return false;
        }
        let mut found = false;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("primary_key") {
                found = true;
            }
            // Skip past any `= ...` value so the parser doesn't error on it.
            if meta.input.peek(syn::Token![=]) {
                let _: syn::Token![=] = meta.input.parse()?;
                let _: proc_macro2::TokenStream = meta.input.parse()?;
            }
            Ok(())
        });
        found
    })
}
