package io.djcode.kafkastreamsexample

import com.fasterxml.jackson.databind.ObjectMapper
import jdk.incubator.vector.VectorOperators.Test
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.ValueMapper
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.event.ApplicationStartedEvent
import org.springframework.boot.runApplication
import org.springframework.context.ApplicationListener
import org.springframework.context.annotation.Bean
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.serializer.JsonSerde
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import java.time.Duration
import java.util.*


@SpringBootApplication
@EnableKafka
@EnableKafkaStreams
class KafkaStreamsExampleApplication

fun main(args: Array<String>) {
	runApplication<KafkaStreamsExampleApplication>(*args)

	@Bean
	fun  hobbit2(): NewTopic {
		return TopicBuilder.name("hobbit2").partitions(1).replicas(1).build()
	}

	@Bean
	fun counts() : NewTopic{
		return TopicBuilder.name("streams-wordcount-output").partitions(1).replicas(1).build()
	}


}



@Component
class Producer(val template: KafkaTemplate<String, User>? = null, val objectMapper: ObjectMapper ): ApplicationListener<ApplicationStartedEvent> {


	override fun onApplicationEvent(event: ApplicationStartedEvent) {
		//faker = Faker.instance()
		val interval: Flux<Long> = Flux.interval(Duration.ofMillis(1000))
		val personasStream = listOf(
				"1Juan",
				"1María",
				"1Pedro"
		).stream()

		val personasFlux: Flux<String> = Flux.fromStream(personasStream.map { it })

		//val quotes: Flux<String> = Flux.fromStream(Stream.generate { Random().nextInt(100).toString() })
	 template!!.send("hobbit", Random().nextInt(100).toString(), User("2","v2"))

	}
}


@Component
internal class Consumer {
	@KafkaListener(topics = ["streams-wordcount-output"], groupId = "spring-boot-kafka")
	fun consume(record: ConsumerRecord<String, User>) {
		println("received = " + record.value() + " with key " + record.key())
	}
}


@Component
class Processor {

	@Autowired
	fun process(builder: StreamsBuilder) {

		// Serializers/deserializers (serde) for String and Long types
		val integerSerde = Serdes.Integer()
		val stringSerde = Serdes.String()
		val longSerde = Serdes.Long()

		// Construct a `KStream` from the input topic "streams-plaintext-input", where message values
		// represent lines of text (for the sake of this example, we ignore whatever may be stored
		// in the message keys).
		val textLines = builder
				.stream("hobbit", Consumed.with(stringSerde, JsonSerde(User::class.java)))
				.mapValues { _, value ->  modifyMessage(value) }
		//val wordCounts = textLines // Split each text line, by whitespace, into words.  The text lines are the message
				// values, i.e. we can ignore whatever data is in the message keys and thus invoke
				// `flatMapValues` instead of the more generic `flatMap`.
		///		.flatMapValues { value: String -> Arrays.asList(*value.lowercase(Locale.getDefault()).split("\\W+".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()) } // We use `groupBy` to ensure the words are available as message keys
		//		.groupBy({ key: Int?, value: String -> value }, Grouped.with(stringSerde, stringSerde)) // Count the occurrences of each word (message key).
		//		.count(Materialized.`as`("counts"))

// Convert the `KTable<String, Long>` into a `KStream<String, Long>` and write to the output topic.
		.to("streams-wordcount-output", Produced.with(stringSerde, JsonSerde(User::class.java)))
	}
}

private fun modifyMessage(value: User): User {
	return User("idx", "valX")
}

@RestController
internal class RestService {
	private val factoryBean: StreamsBuilderFactoryBean? = null
	@GetMapping("/count/{word}")
	fun getCount(@PathVariable word: String): Long {
		val kafkaStreams = factoryBean!!.getKafkaStreams()
		val counts = kafkaStreams!!.store(StoreQueryParameters.fromNameAndType("counts", QueryableStoreTypes.keyValueStore<String, Long>()))
		return counts[word]
	}
}

data class User(val id:String, val value:String)