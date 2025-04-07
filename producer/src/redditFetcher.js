// producer/src/redditFetcher.js
import fetch from "node-fetch"; // 如果使用Node 18+,可省略npm install
import { USER_AGENT, COMMENT_LIMIT } from "./config.js";
import { initDB } from "./db.js";
import { sendToKafka } from "./kafkaProducer.js";

export async function fetchAndProcessRedditData() {
    const db = await initDB();

    try {
        // =============== 新增：从数据库读取所有用户添加的subreddit ===============
        const subredditsRows = await (await db).all("SELECT subreddit FROM user_subreddits");
        // 如果还没添加任何subreddit，就直接返回
        if (!subredditsRows || subredditsRows.length === 0) {
            console.log("No user subreddits found. Nothing to fetch.");
            return;
        }

        // 依次处理每个subreddit
        for (const row of subredditsRows) {
            const subredditName = row.subreddit;
            // 拉取 subreddit 的前5个最热帖子
            const postsUrl = `https://www.reddit.com/r/${subredditName}/hot.json?limit=5`;
            const postsResponse = await fetch(postsUrl, {
                headers: { "User-Agent": USER_AGENT },
            });
            let postsText = await postsResponse.text();
            // 过滤控制字符，避免 JSON 解析错误
            postsText = postsText.replace(/[\x00-\x1F]/g, "");
            const postsJson = JSON.parse(postsText);
            const hotPostItems = postsJson?.data?.children || [];

            // 遍历帖子
            for (const item of hotPostItems) {
                const postDetails = item.data;
                const postId = postDetails.id;
                const subreddit = postDetails.subreddit;
                const title = postDetails.title;

                // 写 posts 表
                await db.run(
                    `INSERT OR REPLACE INTO posts (post_id, title, subreddit)
                     VALUES (?, ?, ?)`,
                    [postId, title, subreddit]
                );

                // 拉取该帖子的评论列表（limit=20, sort=hot）
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

                // 写入 comments 表，并将新插入的发送到Kafka
                for (const c of comments) {
                    const { comment_id, author, body, created_utc } = c;

                    // 使用 INSERT OR IGNORE
                    const result = await db.run(
                        `INSERT OR IGNORE INTO comments (post_id, comment_id, author, body, created_utc)
                         VALUES (?, ?, ?, ?, ?)`,
                        [postId, comment_id, author, body, created_utc]
                    );

                    // 若插入成功 => 发送到Kafka
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