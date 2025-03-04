package ru.quipy.bankDemo.transfers.projections

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Component
import org.springframework.stereotype.Repository
import ru.quipy.bankDemo.transfers.api.*
import ru.quipy.streams.AggregateSubscriptionsManager
import java.util.UUID
import javax.annotation.PostConstruct

@Component
class TransferExistenceCache(
    private val transferCacheRepository: TransferCacheRepository,
    private val subscriptionsManager: AggregateSubscriptionsManager,
) {
    private val logger: Logger = LoggerFactory.getLogger(TransferExistenceCache::class.java)

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(TransferTransactionAggregate::class, "transactions::transfer-cache") {
            `when`(TransferTransactionCreatedEvent::class) { event ->
                transferCacheRepository.save(Transfer(event.transferId, "CREATED"))
                logger.info("Update transfers cache, create transfer ${event.transferId}, status: CREATED")
            }
            `when`(TransactionDeclinedEvent::class) { event ->
                transferCacheRepository.save(Transfer(event.transferId, "DECLINED"))
                logger.info("Update transfers cache, update transfer status ${event.transferId}, status: DECLINED")
            }
            `when`(TransactionConfirmedEvent::class) { event ->
                transferCacheRepository.save(Transfer(event.transferId, "WITHDRAWN"))
                logger.info("Update transfers cache, update transfer status ${event.transferId}, status: WITHDRAWN")
            }
            `when`(TransactionSucceededEvent::class) { event ->
                transferCacheRepository.save(Transfer(event.transferId, "SUCCEEDED"))
                logger.info("Update transfers cache,  transfer status${event.transferId}, status: SUCCEEDED")
            }
            `when`(TransactionFailedEvent::class) { event ->
                transferCacheRepository.save(Transfer(event.transferId, "FAILED"))
                logger.info("Update transfers cache,  transfer status${event.transferId}, status: FAILED")
            }
        }
    }

    fun findById(transferId: UUID): Transfer? {
        return transferCacheRepository.findByTransferId(transferId)
    }
}

@Document("transfer-cache")
data class Transfer(
    @Id
    val transferId: UUID,
    var transferStatus: String,
)

@Repository
interface TransferCacheRepository: MongoRepository<Transfer, UUID> {
    fun findByTransferId(transferId: UUID): Transfer?
}