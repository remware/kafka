package net.remware.k_sample

fun main(args: Array<String>) {
    println("Starting msg-console...")
    JsonProducer("localhost:9092").produce(2)
}