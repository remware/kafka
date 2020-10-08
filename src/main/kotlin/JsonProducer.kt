package net.remware.k_sample

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import mu.KotlinLogging

import java.util.*

class JsonProducer(brokers: String) {
    private val logger = KotlinLogging.logger {}
    private val producer = createProducer(brokers)
    private val channelTopic = "lowercase"
    private val jsonMapper = ObjectMapper().apply {
        registerKotlinModule()
        disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        setDateFormat(StdDateFormat())
    }

    private fun createProducer(brokers: String): Producer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["key.serializer"] = StringSerializer::class.java
        props["value.serializer"] = StringSerializer::class.java
        return KafkaProducer<String, String>(props)
    }

    fun produce(ratePerSecond: Int) {
        val waitTimeBetweenIterationsMs = 1000L / ratePerSecond
        logger.info("Producing $ratePerSecond records per second (1 every ${waitTimeBetweenIterationsMs}ms)")

        while (true) {
            val alarm = Alarm("Equipment failure", 9175114, "LX0005",
                "Osijek", "SubNetwork", "FAULT", "Major")

            logger.info("Generated an alarm: $alarm")

            val generatedJson = jsonMapper.writeValueAsString(alarm)
            logger.debug("JSON data: $generatedJson")

            val futureResult = producer.send(ProducerRecord(channelTopic, generatedJson))
            logger.debug("Sent a record")

            Thread.sleep(waitTimeBetweenIterationsMs)

            // wait for the write acknowledgment
            futureResult.get()
        }
    }
}