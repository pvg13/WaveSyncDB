use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, Fields, Type};

#[proc_macro_derive(CrudModel, attributes(primary_key))]
pub fn derive_crud(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Extract fields from struct
    let fields = if let Data::Struct(data) = &input.data {
        if let Fields::Named(fields) = &data.fields {
            &fields.named
        } else {
            panic!("CrudModel derive only supports named fields");
        }
    } else {
        panic!("CrudModel derive only supports structs");
    };

    // Find the primary key field
    let pk_field = fields.iter().find(|f| {
        f.attrs.iter().any(|attr| attr.path().is_ident("primary_key"))
    }).expect("Must designate one field with #[primary_key]");

    let pk_ident = pk_field.ident.as_ref().unwrap();
    let pk_type = &pk_field.ty;
    
    // Segregate PK from other fields for INSERT/UPDATE logic
    let other_fields: Vec<_> = fields.iter()
        .filter(|f| f.ident.as_ref().unwrap() != pk_ident)
        .collect();
    
    let other_idents: Vec<_> = other_fields.iter().map(|f| f.ident.as_ref().unwrap()).collect();
    let other_names: Vec<_> = other_idents.iter().map(|id| id.to_string()).collect();
    let other_types: Vec<_> = other_fields.iter().map(|f| &f.ty).collect();

    // Metadata for SQL generation
    let table_name = name.to_string().to_lowercase() + "s";
    let columns_joined = other_names.join(", ");
    let placeholders = (1..=other_names.len())
        .map(|i| format!("${}", i))
        .collect::<Vec<_>>()
        .join(", ");

    // SQL for UPDATE: col1 = $2, col2 = $3 (assuming $1 is the PK)
    let set_clause = other_names.iter().enumerate()
        .map(|(i, n)| format!("{} = ${}", n, i + 2))
        .collect::<Vec<_>>()
        .join(", ");

    // Generate SQL schema definitions
    let column_defs: Vec<String> = fields.iter().map(|f| {
        let f_name = f.ident.as_ref().unwrap().to_string();
        let is_pk = f.attrs.iter().any(|a| a.path().is_ident("primary_key"));
        
        if let Type::Path(tp) = &f.ty {
            let type_ident = &tp.path.segments.last().unwrap().ident;
            match type_ident.to_string().as_str() {
                "i32" | "i64" if is_pk => format!("{} INTEGER PRIMARY KEY AUTOINCREMENT", f_name),
                "i32" | "i64" => format!("{} INTEGER", f_name),
                "String" => format!("{} TEXT", f_name),
                "f32" | "f64" => format!("{} REAL", f_name),
                _ => format!("{} BLOB", f_name),
            }
        } else {
            format!("{} BLOB", f_name)
        }
    }).collect();
    let up_columns_joined = column_defs.join(", ");

    // --- Add this to your macro logic before 'let expanded = quote! { ... }' ---
    let other_idents_to_opvalue: Vec<_> = other_fields.iter().map(|f| {
        let ident = f.ident.as_ref().unwrap();
        if let Type::Path(tp) = &f.ty {
            let type_name = tp.path.segments.last().unwrap().ident.to_string();
            match type_name.as_str() {
                "String" => quote! { OpValue::Text(self.#ident.clone()) },
                "i32" | "i64" => quote! { OpValue::Integer(self.#ident as i64) },
                "bool" => quote! { OpValue::Boolean(self.#ident) },
                "f32" | "f64" => quote! { OpValue::Float(self.#ident as f64) },
                _ => quote! { OpValue::Text(format!("{:?}", self.#ident)) }, // Fallback
            }
        } else {
            quote! { OpValue::Text(format!("{:?}", self.#ident)) }
        }
    }).collect();

    let expanded = quote! {
        #[automatically_derived]
        impl #name {
            /// Factory method: Creates the struct and persists it to the database immediately.
            /// Returns the instance with the database-assigned ID populated.
            pub async fn create_new(#(#other_idents: #other_types),*) -> Result<Self, wavesyncdb::crud::CrudError> {
                let mut instance = Self {
                    #pk_ident: Default::default(),
                    #(#other_idents),*
                };
                
                // Persist to DB and update the ID in-place
                instance.create().await?;
                Ok(instance)
            }
        }

        use wavesyncdb::sqlx::Row;

        #[automatically_derived]
        #[wavesyncdb::async_trait::async_trait]
        impl wavesyncdb::crud::Migration for #name {
            async fn up() -> Result<(), wavesyncdb::crud::CrudError> {
                let db = wavesyncdb::DATABASE.get().ok_or(wavesyncdb::crud::CrudError::DbNotInitialized)?;
                wavesyncdb::sqlx::query(&format!(
                    "CREATE TABLE IF NOT EXISTS {} ({})", #table_name, #up_columns_joined
                ))
                .execute(db)
                .await?;
                Ok(())
            }

            async fn down() -> Result<(), wavesyncdb::crud::CrudError> {    
                let db = wavesyncdb::DATABASE.get().ok_or(wavesyncdb::crud::CrudError::DbNotInitialized)?;
                wavesyncdb::sqlx::query(&format!("DROP TABLE IF EXISTS {}", #table_name))
                    .execute(db)
                    .await?;
                Ok(())
            }
        }

        #[automatically_derived]
        #[wavesyncdb::async_trait::async_trait]
        impl wavesyncdb::crud::CrudModel for #name {
            type PrimaryKey = #pk_type;

            async fn create(&mut self) -> Result<(), wavesyncdb::crud::CrudError> {
                let db = wavesyncdb::DATABASE.get().ok_or(wavesyncdb::crud::CrudError::DbNotInitialized)?;
                
                // SQLlite/Postgres standard for returning the auto-generated ID
                let sql = format!(
                    "INSERT INTO {} ({}) VALUES ({}) RETURNING {}", 
                    #table_name, 
                    #columns_joined, 
                    #placeholders,
                    stringify!(#pk_ident)
                );
                
                let row = wavesyncdb::sqlx::query(&sql)
                    #( .bind(&self.#other_idents) )*
                    .fetch_one(db)
                    .await?;

                // Map the returned ID back to the struct
                self.#pk_ident = row.try_get(stringify!(#pk_ident))?;

                Ok(())
            }

            async fn read(&mut self) -> Result<(), wavesyncdb::crud::CrudError> {
                let db = wavesyncdb::DATABASE.get().ok_or(wavesyncdb::crud::CrudError::DbNotInitialized)?;
                let row = wavesyncdb::sqlx::query(&format!("SELECT * FROM {} WHERE {} = $1", #table_name, stringify!(#pk_ident)))
                    .bind(&self.#pk_ident)
                    .fetch_one(db)
                    .await?;

                #( self.#other_idents = row.try_get(stringify!(#other_idents))?; )*
                Ok(())
            }

            async fn update(&mut self) -> Result<(), wavesyncdb::crud::CrudError> {
                let db = wavesyncdb::DATABASE.get().ok_or(wavesyncdb::crud::CrudError::DbNotInitialized)?;
                wavesyncdb::sqlx::query(&format!("UPDATE {} SET {} WHERE {} = $1", #table_name, #set_clause, stringify!(#pk_ident)))
                    .bind(&self.#pk_ident)
                    #( .bind(&self.#other_idents) )*
                    .execute(db)
                    .await?;
                Ok(())
            }

            async fn delete(self) -> Result<(), wavesyncdb::crud::CrudError> {
                Self::delete_by_id(self.#pk_ident).await
            }

            async fn get(id: Self::PrimaryKey) -> Result<Self, wavesyncdb::crud::CrudError> {
                let db = wavesyncdb::DATABASE.get().ok_or(wavesyncdb::crud::CrudError::DbNotInitialized)?;
                let row = wavesyncdb::sqlx::query(&format!("SELECT * FROM {} WHERE {} = $1", #table_name, stringify!(#pk_ident)))
                    .bind(&id)
                    .fetch_one(db)
                    .await?;

                Ok(#name {
                    #pk_ident: id,
                    #( #other_idents: row.try_get(stringify!(#other_idents))?, )*
                })
            }

            async fn all() -> Result<Vec<Self>, wavesyncdb::crud::CrudError> {
                use wavesyncdb::sqlx::Row;
                let db = wavesyncdb::DATABASE.get().ok_or(wavesyncdb::crud::CrudError::DbNotInitialized)?;
                let rows = wavesyncdb::sqlx::query(&format!("SELECT * FROM {}", #table_name))
                    .fetch_all(db)
                    .await?;

                Ok(rows.into_iter().map(|row| #name {
                    #pk_ident: row.get(stringify!(#pk_ident)),
                    #( #other_idents: row.get(stringify!(#other_idents)), )*
                }).collect())
            }

            async fn delete_by_id(id: Self::PrimaryKey) -> Result<(), wavesyncdb::crud::CrudError> {
                let db = wavesyncdb::DATABASE.get().ok_or(wavesyncdb::crud::CrudError::DbNotInitialized)?;
                wavesyncdb::sqlx::query(&format!("DELETE FROM {} WHERE {} = $1", #table_name, stringify!(#pk_ident)))
                    .bind(id)
                    .execute(db)
                    .await?;
                Ok(())
            }

            fn id(&self) -> Self::PrimaryKey {
                self.#pk_ident.clone()
            }

            fn table_name() -> String {
                #table_name.to_string()
            }

            fn fields(&self) -> Vec<(String, wavesyncdb::messages::OpValue)> {
                use wavesyncdb::messages::OpValue;
                vec![
                    #(
                        (stringify!(#other_idents).to_string(), #other_idents_to_opvalue)
                    ),*
                ]
            }
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(SyncedCrudModel, attributes(primary_key))]
pub fn derive_synced(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let fields = if let Data::Struct(data) = &input.data {
        if let Fields::Named(fields) = &data.fields {
            &fields.named
        } else {
            panic!("SyncedCrudModel only supports named fields");
        }
    } else {
        panic!("SyncedCrudModel only supports structs");
    };

    // Find the primary key field
    let pk_field = fields.iter().find(|f| {
        f.attrs.iter().any(|attr| attr.path().is_ident("primary_key"))
    }).expect("Must designate one field with #[primary_key]");

    let pk_ident = pk_field.ident.as_ref().unwrap();
    let pk_type = &pk_field.ty;

    // Preserve ordering by filtering into a single vector first
    let other_fields: Vec<_> = fields.iter()
        .filter(|f| f.ident.as_ref().unwrap() != pk_ident)
        .collect();
    
    // Deconstruct fields into parallel vectors for quote repetition
    let other_idents: Vec<_> = other_fields.iter().map(|f| f.ident.as_ref().unwrap()).collect();
    let other_types: Vec<_> = other_fields.iter().map(|f| &f.ty).collect();

    let expanded = quote! {
        #[automatically_derived]
        impl #name {
            /// Factory: Creates locally, assigns DB ID, and syncs to Engine.
            /// Params order: #(#other_idents),*
            pub async fn create_synced_new(
                #(#other_idents: #other_types),*
            ) -> Result<Self, wavesyncdb::SyncError> {
                // 1. Local Instantiation
                let mut instance = Self {
                    #pk_ident: Default::default(),
                    #( #other_idents ),*
                };

                // 2. Local DB Persist (requires CrudModel)
                // This call uses RETURNING to update instance.#pk_ident
                wavesyncdb::crud::CrudModel::create(&mut instance)
                    .await
                    .map_err(wavesyncdb::SyncError::CrudError)?;

                // 3. Engine Sync (requires SyncedModel blanket impl)
                // This sends the populated fields and the new ID to ENGINE_TX
                wavesyncdb::SyncedModel::sync_create(&mut instance).await?;

                Ok(instance)
            }
        }
    };

    TokenStream::from(expanded)
}