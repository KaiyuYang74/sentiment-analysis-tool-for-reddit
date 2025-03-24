export const DB_PATH = "/Users/kaiyuyang/Desktop/redditData.db";

// Kafka相关配置
export const KAFKA_BROKERS = ["localhost:9092"]; // 你实际的Kafka broker地址
export const KAFKA_TOPIC = "reddit_comments"; // 你要使用的Topic名称
// export const KAFKA_RETENTION_MS = 10800000; // 3小时的retention time

// Reddit相关拉取配置
export const USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)";
export const REDDIT_HOT_POSTS_URL =
    "https://www.reddit.com/r/all/hot.json?limit=5";
// ^ 这里改成 limit=5，获取5个最热贴

// 每个贴子评论的limit
export const COMMENT_LIMIT = 20;

export const FETCH_INTERVAL_MS = 1 * 60 * 1000; // 每1分钟执行一次
