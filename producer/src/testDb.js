// testDb.js
import { initDB } from "./db.js";

(async () => {
    try {
        // 初始化数据库连接
        const db = await initDB();

        // 尝试执行一条简单的 SQL 查询，比如查看目前都有哪些表:
        const row = await db.get(
            "SELECT name FROM sqlite_master WHERE type='table' LIMIT 1"
        );
        if (row) {
            console.log("数据库连接正常，示例表信息:", row);
        } else {
            console.log("数据库连接正常，但未查询到任何表信息。");
        }
    } catch (error) {
        console.error("数据库连接失败，错误信息:", error);
    }
})();
