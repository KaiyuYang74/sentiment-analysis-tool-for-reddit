// producer/src/redditFetcher.js
import fetch from "node-fetch"; // For Node 18+, this import can be omitted
import { USER_AGENT, COMMENT_LIMIT } from "./config.js";
import { initDB } from "./db.js";
import { sendToKafka } from "./kafkaProducer.js";

export async function fetchAndProcessRedditData() {
    const db = await initDB();

    try {
        // =============== Added: Fetch all user-added subreddits from database ===============
        const subredditsRows = await (
            await db
        ).all("SELECT subreddit FROM user_subreddits");
        // If no subreddits added yet, return early
        if (!subredditsRows || subredditsRows.length === 0) {
            console.log("No user subreddits found. Nothing to fetch.");
            return;
        }

        // Process each subreddit
        for (const row of subredditsRows) {
            const subredditName = row.subreddit;
            // Fetch top 5 hot posts from subreddit
            const postsUrl = `https://www.reddit.com/r/${subredditName}/hot.json?limit=5`;
            const postsResponse = await fetch(postsUrl, {
                headers: { "User-Agent": USER_AGENT },
            });
            let postsText = await postsResponse.text();
            // Filter control characters to avoid JSON parsing errors
            postsText = postsText.replace(/[\x00-\x1F]/g, "");
            const postsJson = JSON.parse(postsText);
            const hotPostItems = postsJson?.data?.children || [];

            // Process each post
            for (const item of hotPostItems) {
                const postDetails = item.data;
                const postId = postDetails.id;
                const subreddit = postDetails.subreddit;
                const title = postDetails.title;

                // Insert to posts table
                await db.run(
                    `INSERT OR REPLACE INTO posts (post_id, title, subreddit)
                     VALUES (?, ?, ?)`,
                    [postId, title, subreddit]
                );

                // Fetch comments for this post (limit=20, sort=hot)
                const commentsUrl = `https://www.reddit.com/r/${subreddit}/comments/${postId}.json?limit=${COMMENT_LIMIT}&sort=hot`;
                const commentsResponse = await fetch(commentsUrl, {
                    headers: { "User-Agent": USER_AGENT },
                });
                let commentsText = await commentsResponse.text();
                commentsText = commentsText.replace(/[\x00-\x1F]/g, "");
                const commentsJson = JSON.parse(commentsText);

                // Reddit comments structure: [0]->post details, [1]->comments list
                const commentsListing = commentsJson[1]?.data?.children || [];
                let comments = commentsListing
                    .filter((child) => child.kind === "t1") // Keep only actual comments
                    .map((child) => {
                        const data = child.data;
                        return {
                            comment_id: data.id,
                            author: data.author,
                            body: data.body,
                            created_utc: data.created_utc,
                            score: data.score,
                        };
                    });

                // Sort by score descending, take top 5 comments
                comments.sort((a, b) => b.score - a.score);
                comments = comments.slice(0, 5);

                // Remove score field if not needed
                comments = comments.map(({ score, ...rest }) => rest);

                // Insert to comments table and send new ones to Kafka
                for (const c of comments) {
                    const { comment_id, author, body, created_utc } = c;

                    // Use INSERT OR IGNORE
                    const result = await db.run(
                        `INSERT OR IGNORE INTO comments (post_id, comment_id, author, body, created_utc)
                         VALUES (?, ?, ?, ?, ?)`,
                        [postId, comment_id, author, body, created_utc]
                    );

                    // If inserted successfully => send to Kafka
                    if (result.changes === 1) {
                        const message = {
                            post_id: postId,
                            comment_id,
                            author,
                            body,
                            created_utc,
                        };
                        await sendToKafka(message);
                    } else {
                        console.log("Comment already exists:", comment_id);
                    }
                }
            }
        }
    } catch (error) {
        console.error("Error in fetchAndProcessRedditData:", error);
    }
}
