import { Kafka } from "kafkajs";
import { KAFKA_BROKERS, KAFKA_TOPIC } from "./config.js";

let producer;

export async function initKafkaProducer() {
    //init a kafka client
    const kafka = new Kafka({
        clientId: "reddit-producer-client",
        brokers: KAFKA_BROKERS,
    });

    producer = kafka.producer();
    await producer.connect();
    console.log("[Kafka] Producer connected.");
}

export async function sendToKafka(messageObject) {
    if (!producer) {
        throw new Error(
            "Kafka producer is not initialized. Call initKafkaProducer() first."
        );
    }

    const messageString = JSON.stringify(messageObject);
    try {
        await producer.send({
            topic: KAFKA_TOPIC,
            messages: [{ value: messageString }],
        });
        console.log("[Kafka] Sent message:", messageString);
    } catch (err) {
        console.error("[Kafka] Error sending message:", err);
    }
}
