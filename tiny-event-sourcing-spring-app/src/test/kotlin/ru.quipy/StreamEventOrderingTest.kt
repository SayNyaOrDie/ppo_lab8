package ru.quipy

import org.awaitility.kotlin.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration
import ru.quipy.config.DockerPostgresDataSourceInitializer
import ru.quipy.core.EventSourcingProperties
import ru.quipy.core.EventSourcingService
import ru.quipy.projectDemo.api.ProjectAggregate
import ru.quipy.projectDemo.api.TagCreatedEvent
import ru.quipy.projectDemo.create
import ru.quipy.projectDemo.createTag
import ru.quipy.projectDemo.logic.ProjectAggregateState
import ru.quipy.streams.AggregateSubscriptionsManager
import java.util.concurrent.TimeUnit

@SpringBootTest
@ActiveProfiles("test")
@ContextConfiguration(
    initializers = [DockerPostgresDataSourceInitializer::class])
@EnableAutoConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class StreamEventOrderingTest: BaseTest(testId) {
    companion object {
        const val testId = "StreamEventOrderingTest"
    }
    private val properties: EventSourcingProperties = EventSourcingProperties(
        streamBatchSize = 3
    )
    @Autowired
    private lateinit var esService: EventSourcingService<String, ProjectAggregate, ProjectAggregateState>

    @Autowired
    private lateinit var subscriptionsManager: AggregateSubscriptionsManager

    private val sb = StringBuilder()

    @BeforeEach
    fun init() {
        cleanDatabase()
    }

    @Test
    fun testEventOrder() {
        esService.create {
            it.create(testId)
        }

        esService.update(testId) {
            it.createTag("1")
        }
        esService.update(testId) {
            it.createTag("2")
        }
        esService.update(testId) {
            it.createTag("3")
        }
        esService.update(testId) {
            it.createTag("4")
        }
        esService.update(testId) {
            it.createTag("5")
        }
        esService.update(testId) {
            it.createTag("6")
        }

        subscriptionsManager.createSubscriber(ProjectAggregate::class, "StreamEventOrderingTest") {
            `when`(TagCreatedEvent::class) { event ->
                sb.append(event.tagName).also {
                    println(sb.toString())
                }
            }
        }

        await.atMost(10, TimeUnit.MINUTES).until {
            sb.toString() == "123456"
        }
    }
}