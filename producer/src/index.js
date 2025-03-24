import { initDB } from "./db.js";
import { initKafkaProducer } from "./kafkaProducer.js";
import { fetchAndProcessRedditData } from "./redditFetcher.js";
import { FETCH_INTERVAL_MS } from "./config.js";

async function main() {
    try {
        // 1) 初始化数据库连接
        await initDB();

        // 2) 初始化Kafka Producer
        await initKafkaProducer();

        // 3) 启动周期性任务，比如每10分钟执行一次
        //   (下面示例用setInterval简单实现，生产环境可考虑node-cron等更完善的调度方式)
        // 先立即执行一次，然后开始轮询
        await fetchAndProcessRedditData();
        setInterval(async () => {
            console.log("=== Starting new fetch cycle ===");
            await fetchAndProcessRedditData();
        }, FETCH_INTERVAL_MS);
    } catch (error) {
        console.error("Producer initialization error:", error);
        process.exit(1);
    }
}

main();
