CREATE TABLE haex_crdt_configs_no_sync (
    key TEXT PRIMARY KEY NOT NULL,
    type TEXT NOT NULL,
    value TEXT NOT NULL
);
--> statement-breakpoint
CREATE TABLE haex_crdt_dirty_tables_no_sync (
    table_name TEXT PRIMARY KEY NOT NULL,
    last_modified TEXT
);
--> statement-breakpoint
CREATE TABLE haex_deleted_rows (
    id TEXT PRIMARY KEY NOT NULL,
    table_name TEXT NOT NULL,
    row_pks TEXT NOT NULL,
    haex_hlc TEXT,
    haex_column_hlcs TEXT NOT NULL DEFAULT '{}',
    haex_column_sigs TEXT NOT NULL DEFAULT '{}'
);
