package ru.quipy.bankDemo.transfers.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import ru.quipy.bankDemo.accounts.api.*
import ru.quipy.bankDemo.transfers.api.TransferTransactionAggregate
import ru.quipy.bankDemo.transfers.logic.TransferTransaction
import ru.quipy.core.EventSourcingService
import ru.quipy.saga.SagaManager
import ru.quipy.streams.AggregateSubscriptionsManager
import java.util.*
import javax.annotation.PostConstruct

@Component
class BankAccountsSubscriber(
    private val subscriptionsManager: AggregateSubscriptionsManager,
    private val transactionEsService: EventSourcingService<UUID, TransferTransactionAggregate, TransferTransaction>,
    private val sagaManager: SagaManager
) {
    private val logger: Logger = LoggerFactory.getLogger(BankAccountsSubscriber::class.java)

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(AccountAggregate::class, "transactions::bank-accounts-subscriber") {
//            успешное снятие
            `when`(TransferTransactionAcceptedEvent::class) { event ->
                val sagaContext = sagaManager
                    .withContextGiven(event.sagaContext)
                    .performSagaStep("TRANSFER_PROCESSING", "process withdrawal")
                    .sagaContext()
                transactionEsService.update(event.transactionId, sagaContext) {
                    it.processTransferAccept(event.bankAccountId)
                }
            }
            `when`(TransferTransactionDeclinedEvent::class) { event ->
                val sagaContext = sagaManager
                    .withContextGiven(event.sagaContext)
                    .performSagaStep("TRANSFER_PROCESSING", "abort transfer")
                    .sagaContext()
                transactionEsService.update(event.transactionId, sagaContext) {
                    it.processTransferDecline(event.bankAccountId)
                }
            }
//            успешный деп
            `when`(TransferTransactionProcessedEvent::class) { event ->
                val sagaContext = sagaManager
                    .withContextGiven(event.sagaContext)
                    .performSagaStep("TRANSFER_PROCESSING", "process deposit")
                    .sagaContext()
                transactionEsService.update(event.transactionId, sagaContext) {
                    it.participantCommitted(event.bankAccountId)
                }
            }
            `when`(TransferTransactionRollbackedEvent::class) { event ->
                val sagaContext = sagaManager
                    .withContextGiven(event.sagaContext)
                    .performSagaStep("TRANSFER_PROCESSING", "rollback transfer")
                    .sagaContext()
                transactionEsService.update(event.transactionId, sagaContext) {
                    it.transferRollbacked(event.bankAccountId)
                }
            }
        }
    }
}