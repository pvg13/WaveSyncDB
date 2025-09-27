# WaveSyncDB

**WaveSyncDB** is an experimental **database synchronization layer** for Rust applications.  
It wraps your **Diesel** connection and adds transparent, background synchronization across devices using **libp2p**.

---

## ✨ Features

- **Diesel Integration** – instrument your existing Diesel connection; queries and changes are tracked automatically.  
- **Transparent Sync** – applications keep using Diesel as usual; WaveSyncDB takes care of change logging and replication.  
- **libp2p Networking** – peer-to-peer synchronization with discovery and messaging handled via libp2p.  
- **Conflict Strategy** – single-writer-per-object (SWPO) model ensures consistency without heavy coordination.  

---

## 🚧 Roadmap

- Support for more ORMs/drivers (SQLx, Drizzle, Room, etc.)  
- Pluggable storage backends (SQLite, Postgres, IndexedDB, RocksDB)  
- Flexible transports (QUIC, WebRTC, relay servers)  
- CRDT-based conflict resolution for collaborative editing  

---

## 🔧 Use Case

If you have a Rust application using **Diesel** and want to experiment with **peer-to-peer sync** without changing your database code, WaveSyncDB is for you.

---

## 📦 Installation

Add WaveSyncDB to your `Cargo.toml`:

```toml
[dependencies]
wavesyncdb = { git = "https://github.com/pvg13/wavesyncdb" }
```