// @generated automatically by Diesel CLI.

diesel::table! {
    tasks (id) {
        id -> Nullable<Integer>,
        title -> Text,
        description -> Nullable<Text>,
        completed -> Bool,
        created_at -> Nullable<Timestamp>,
        updated_at -> Nullable<Timestamp>,
    }
}
