// producer/src/db.js
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import { DB_PATH } from "./config.js";

// Create and export a database connection instance
let dbInstance;

export async function initDB() {
    if (!dbInstance) {
        dbInstance = await open({
            filename: DB_PATH,
            driver: sqlite3.Database,
        });
        // Can do some checks here, like if tables already exist
        // But in the problem description, tables are already created

        // =============== Added: Auto-create tables if they don't exist ===============
        await dbInstance.run(`
          CREATE TABLE IF NOT EXISTS user_subreddits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subreddit TEXT UNIQUE NOT NULL
          )
        `);
    }
    return dbInstance;
}
