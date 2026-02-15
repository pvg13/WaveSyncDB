
use wavesyncdb::CrudModel;

// There are two types of signals, one that checks updates in an specific row
// And another that checks updates in the whole table, this is useful for tables that have a lot of rows and we want to check updates in the whole table instead of checking each row individually mod database; mod signals; pub use database::*; pub use signals::*;

pub trait TableStorage: Clone + 'static {
    type TableName: Into<String> + 'static;

    fn get_all<T: CrudModel>(table: &Self::TableName) -> Vec<T>;
    fn set<T: CrudModel>(&self, item: T);
    fn delete<T: CrudModel>(&self, id: i32);
    fn update<T: CrudModel>(&self, item: T);
}

pub trait TableSubscriber<S: TableStorage> {
    fn subscribe<T: CrudModel>(&self, table: &S::TableName) -> Vec<T>;
}