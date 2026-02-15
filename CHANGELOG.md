# Changelog

### 09/02/2026
- Changed the format from instrumenting an existing database connection to implementing basic Crud operations to simplify development and add custom features
- Added the `Crud` trait to implement basic CRUD operations
- Added the `Synced` traid to implement the syncronization features on top of crud
- Changed the SyncEngine to use an static TX channel and the new Crud trait to handle syncronization
- Replaced `diesel` for `sqlx` as the main sql database connection
- Added the `Operation` enum as the main synced message to sync and execute the network operations safetly (I hope to protect better against sql injections)
- Started to sketch the derive trait to easily implement the `Crud` and `Synced` traits


### 10/02/2026
- Fixed some derive erros
- Tested p2p example: The data does not correctly sync :(

### 12/02/2026
- Added dioxus signals