// producer/src/db.js
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import { DB_PATH } from "./config.js";

// 创建并导出一个数据库连接实例
let dbInstance;

export async function initDB() {
    if (!dbInstance) {
        dbInstance = await open({
            filename: DB_PATH,
            driver: sqlite3.Database,
        });
        // 这里可以做一些检查，比如是否已经建表
        // 不过你在问题描述中已经建好表，所以此处略过
    }
    return dbInstance;
}
