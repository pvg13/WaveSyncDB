use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input, spanned::Spanned};

/// Derive macro that
///
/// 1. registers a SeaORM entity for auto-discovery by
///    `WaveSyncDb::get_schema_registry`, and
/// 2. emits an `impl wavesyncdb::SyncedModel for Model` so the Dioxus
///    reactive hooks can apply per-column changes from `ChangeNotification`
///    in place — no DB round-trip on the receive path.
///
/// Place this alongside `DeriveEntityModel` on your `Model` struct:
///
/// ```ignore
/// #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, SyncEntity)]
/// #[sea_orm(table_name = "tasks")]
/// pub struct Model { ... }
/// ```
#[proc_macro_derive(SyncEntity, attributes(sea_orm))]
pub fn derive_sync_entity(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

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

    let synced_model_impl = match build_synced_model_impl(&input) {
        Ok(ts) => ts,
        Err(e) => return e.to_compile_error().into(),
    };

    let model_ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    quote! {
        #inventory_block

        #[automatically_derived]
        impl #impl_generics wavesyncdb::SyncedModel for #model_ident #ty_generics #where_clause {
            #synced_model_impl
        }
    }
    .into()
}

/// Walks the struct's named fields, finds the field marked
/// `#[sea_orm(primary_key)]`, and emits the three trait-method bodies.
fn build_synced_model_impl(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
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

    // Local binding names used inside `wavesync_from_changes` to avoid
    // shadowing the struct field name when constructing the final value.
    let local_idents: Vec<syn::Ident> = field_idents
        .iter()
        .map(|i| syn::Ident::new(&format!("__ws_{}", i), i.span()))
        .collect();
    let pk_local = local_idents
        .iter()
        .zip(field_idents.iter())
        .find(|(_, fi)| **fi == pk_ident)
        .map(|(li, _)| li.clone())
        .expect("pk ident must be in field list");

    Ok(quote! {
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
    })
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
