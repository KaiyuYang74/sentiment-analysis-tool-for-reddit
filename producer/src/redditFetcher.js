// producer/src/redditFetcher.js
import fetch from "node-fetch"; // 如果使用Node 18+,可省略npm install
import { USER_AGENT, REDDIT_HOT_POSTS_URL, COMMENT_LIMIT } from "./config.js";
import { initDB } from "./db.js";
import { sendToKafka } from "./kafkaProducer.js";

export async function fetchAndProcessRedditData() {
    const db = await initDB();

    try {
        // 1) 拉取最热帖子列表（5个）
        const postsResponse = await fetch(REDDIT_HOT_POSTS_URL, {
            headers: { "User-Agent": USER_AGENT },
        });
        let postsText = await postsResponse.text();
        // 过滤控制字符，避免 JSON 解析错误
        postsText = postsText.replace(/[\x00-\x1F]/g, "");
        const postsJson = JSON.parse(postsText);
        const hotPostItems = postsJson?.data?.children || [];

        // 2) 遍历帖子，分别拉取其评论
        for (const item of hotPostItems) {
            const postDetails = item.data;
            const postId = postDetails.id;
            const subreddit = postDetails.subreddit;
            const title = postDetails.title;

            // 2.1) 先写 posts 表：INSERT OR REPLACE
            await db.run(
                `INSERT OR REPLACE INTO posts (post_id, title, subreddit)
         VALUES (?, ?, ?)`,
                [postId, title, subreddit]
            );

            // 2.2) 获取该帖子的评论列表
            const commentsUrl = `https://www.reddit.com/r/${subreddit}/comments/${postId}.json?limit=${COMMENT_LIMIT}&sort=hot`;
            const commentsResponse = await fetch(commentsUrl, {
                headers: { "User-Agent": USER_AGENT },
            });
            let commentsText = await commentsResponse.text();
            commentsText = commentsText.replace(/[\x00-\x1F]/g, "");
            const commentsJson = JSON.parse(commentsText);

            // Reddit返回的comments结构: [0]->帖子的详情, [1]->评论列表
            const commentsListing = commentsJson[1]?.data?.children || [];
            let comments = commentsListing
                .filter((child) => child.kind === "t1") // 保留实际评论
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

            // 按 score 降序排，取前5个评论
            comments.sort((a, b) => b.score - a.score);
            comments = comments.slice(0, 5);

            // 如果不需要 score 字段，可以移除
            comments = comments.map(({ score, ...rest }) => rest);

            // 3) 逐条写入 comments 表，并将新插入的发送到Kafka
            for (const c of comments) {
                const { comment_id, author, body, created_utc } = c;

                // 使用 INSERT OR IGNORE
                const result = await db.run(
                    `INSERT OR IGNORE INTO comments (post_id, comment_id, author, body, created_utc)
           VALUES (?, ?, ?, ?, ?)`,
                    [postId, comment_id, author, body, created_utc]
                );

                // sqlite3 (INSERT OR IGNORE) 若插入成功，changes=1；若已存在则changes=0
                if (result.changes === 1) {
                    // 说明是新评论，需要发送到Kafka
                    // 注意：你可以在发送给Kafka的消息中，包含post的元信息或只发comment均可
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
    } catch (error) {
        console.error("Error in fetchAndProcessRedditData:", error);
    }
}
