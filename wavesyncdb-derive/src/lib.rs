use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Data, Fields};

#[proc_macro_derive(Crud, attributes(primary_key))] // Note: attributes(primary_key) is required
pub fn derive_crud(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let fields = if let Data::Struct(data) = &input.data {
        if let Fields::Named(fields) = &data.fields {
            &fields.named
        } else {
            panic!("Crud derive only supports named fields");
        }
    } else {
        panic!("Crud derive only supports structs");
    };

    // --- NEW LOGIC: Find the field with #[primary_key] ---
    let pk_field = fields.iter().find(|f| {
        f.attrs.iter().any(|attr| attr.path().is_ident("primary_key"))
    }).expect("Must designate one field with #[primary_key]");

    let pk_ident = pk_field.ident.as_ref().unwrap(); // e.g., 'user_id'
    let pk_type = &pk_field.ty;                    // e.g., 'u64'
    
    // Extract everything else for columns (usually you exclude the PK from auto-inserts if it's Serial/Autoincrement)
    let other_fields: Vec<_> = fields.iter().filter(|f| f.ident.as_ref().unwrap() != pk_ident).collect();
    let other_idents: Vec<_> = other_fields.iter().map(|f| f.ident.as_ref().unwrap().clone()).collect();
    let other_names: Vec<_> = other_idents.iter().map(|id| id.to_string()).collect();
    let table_name = name.to_string().to_lowercase() + "s";
    let columns_str = other_names.join(", ");
    let placeholders = (1..=other_names.len()).map(|i| format!("${}", i)).collect::<Vec<_>>().join(", ");

    let expanded = quote! {
        #[async_trait::async_trait]
        impl Crud for #name {
            // Use the type found in the struct!
            type PrimaryKey = #pk_type;

            fn id(&self) -> Self::PrimaryKey {
                self.#pk_ident.clone()
            }

            async fn get(id: Self::PrimaryKey) -> Result<Self, CrudError> {
                let sql = format!("SELECT * FROM {} WHERE {} = $1", Self::table_name(), stringify!(#pk_ident));
                let res = sqlx::query_as::<sqlx::Sqlite, Self>(&sql)
                    .bind(id)
                    .fetch_one(DATABASE.get().unwrap())
                    .await?;
                Ok(res)
            }

            async fn create(value: &Self) -> Result<(), CrudError> {
                let sql = format!("INSERT INTO {} ({}) VALUES ({})", 
                    Self::table_name(), #columns_str, #placeholders);
                let table_name = name.to_string().to_lowercase() + "s";
    let columns_str = other_names.join(", ");
    let placeholders = (1..=other_names.len()).map(|i| format!("${}", i)).collect::<Vec<_>>().join(", ");

    let expanded = quote! {
        #[async_trait::async_trait]
        impl Crud for #name {
            // Use the type found in the struct!
            type PrimaryKey = #pk_type;

            fn id(&self) -> Self::PrimaryKey {
                self.#pk_ident.clone()
            }

            async fn get(id: Self::PrimaryKey) -> Result<Self, CrudError> {
                let sql = format!("SELECT * FROM {} WHERE {} = $1", Self::table_name(), stringify!(#pk_ident));
                let res = sqlx::query_as::<sqlx::Sqlite, Self>(&sql)
                    .bind(id)
                    .fetch_one(DATABASE.get().unwrap())
                    .await?;
                Ok(res)
            }

            async fn create(value: &Self) -> Result<(), CrudError> {
                let sql = format!("INSERT INTO {} ({}) VALUES ({})", 
                    Self::table_name(), #columns_str, #placeholders);
                
                sqlx::query(&sql)
                    #( .bind(&value.#other_idents) )*
                    .execute(DATABASE.get().unwrap())
                    .await?;
                Ok(())
            }
            
            // ... apply similar logic to update/delete ...
        }
    };
                sqlx::query(&sql)
                    #( .bind(&value.#other_idents) )*
                    .execute(DATABASE.get().unwrap())
                    .await?;
                Ok(())
            }
            
            // ... apply similar logic to update/delete ...
        }
    };

    TokenStream::from(expanded)
}