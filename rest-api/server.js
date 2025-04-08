// rest-app/server.js

import express from "express";
import cors from "cors";
import path from "path";
import { fileURLToPath } from "url";
import { initDB } from "../producer/src/db.js";

const app = express();
app.use(cors());
app.use(express.json());

// Make static files in public directory accessible
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
app.use(express.static(path.join(__dirname, "public")));

// ========== 1) Get added subreddits ==========
app.get("/api/subreddits", async (req, res) => {
    try {
        const db = await initDB();
        const rows = await db.all("SELECT subreddit FROM user_subreddits");
        const subreddits = rows.map((r) => r.subreddit);
        res.json({ subreddits });
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Failed to fetch subreddits" });
    }
});

// ========== 2) Add new subreddit ==========
app.post("/api/subreddits", async (req, res) => {
    try {
        const { subreddit } = req.body;
        if (!subreddit) {
            return res.status(400).json({ error: "No subreddit specified" });
        }
        const db = await initDB();
        await db.run(
            "INSERT OR IGNORE INTO user_subreddits (subreddit) VALUES (?)",
            [subreddit]
        );
        res.json({ success: true });
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Failed to add subreddit" });
    }
});

// ========== 3) Get analysis results (analysis_results) ==========
app.get("/api/results", async (req, res) => {
    try {
        const db = await initDB();
        // Join with posts table to get subreddit
        const rows = await db.all(`
      SELECT
        a.post_id,
        p.title,
        p.subreddit,
        a.word_freq_json,
        a.sentiment_result,
        a.updated_time
      FROM analysis_results a
      JOIN posts p ON a.post_id = p.post_id
      ORDER BY a.updated_time DESC
    `);
        res.json(rows);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Failed to fetch results" });
    }
});

// ========== Start server ==========
const PORT = process.env.PORT || 3003;
app.listen(PORT, () => {
    console.log(`REST app listening on port ${PORT}`);
});
