package ru.quipy.bankDemo.accounts.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import ru.quipy.bankDemo.accounts.api.AccountAggregate
import ru.quipy.bankDemo.transfers.api.TransferTransactionAggregate
import ru.quipy.bankDemo.transfers.api.TransferTransactionCreatedEvent
import ru.quipy.bankDemo.accounts.logic.Account
import ru.quipy.bankDemo.transfers.api.TransactionConfirmedEvent
import ru.quipy.bankDemo.transfers.api.TransactionFailedEvent
import ru.quipy.core.EventSourcingService
import ru.quipy.saga.SagaManager
import ru.quipy.streams.AggregateSubscriptionsManager
import java.util.*
import javax.annotation.PostConstruct

@Component
class TransactionsSubscriber(
    private val subscriptionsManager: AggregateSubscriptionsManager,
    private val accountEsService: EventSourcingService<UUID, AccountAggregate, Account>,
    private val sagaManager: SagaManager
) {
    private val logger: Logger = LoggerFactory.getLogger(TransactionsSubscriber::class.java)

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(TransferTransactionAggregate::class, "accounts::transaction-processing-subscriber") {
            `when`(TransferTransactionCreatedEvent::class) { event ->
                logger.info("Got transaction to process: $event")
                logger.info("Starting account withdrawal: ${event.sourceBankAccountId}")

                val sagaContext = sagaManager
                    .withContextGiven(event.sagaContext)
                    .performSagaStep("TRANSFER_PROCESSING", "withdraw source")
                    .sagaContext()

                val transactionOutcome = accountEsService.update(event.sourceAccountId, sagaContext) {
                    it.performTransferFrom(
                        event.sourceBankAccountId,
                        event.transferId,
                        event.transferAmount
                    )
                }

                logger.info("Transaction: ${event.transferId}. Outcome: $transactionOutcome")
            }
            `when`(TransactionConfirmedEvent::class) { event ->
                logger.info("Starting account replenishment: ${event.destinationAccountId}")

                val sagaContext = sagaManager
                    .withContextGiven(event.sagaContext)
                    .performSagaStep("TRANSFER_PROCESSING", "deposit target")
                    .sagaContext()

                val transactionOutcome = accountEsService.update(event.destinationAccountId, sagaContext) {
                    it.performTransferTo(
                        event.destinationBankAccountId,
                        event.transferId,
                        event.transferAmount
                    )
                }

                logger.info("Transaction: ${event.transferId}. Outcome: $transactionOutcome")
            }
            `when`(TransactionFailedEvent::class) { event ->
                logger.info("Transaction failed, rolling back: $event")

                val sagaContext = sagaManager
                    .withContextGiven(event.sagaContext)
                    .performSagaStep("TRANSFER_PROCESSING", "cancel withdrawal")
                    .sagaContext()

                val transactionOutcome = accountEsService.update(event.sourceAccountId, sagaContext) {
                    it.rollbackTransaction(event.sourceBankAccountId, event.transferId, event.transferAmount)
                }

                logger.info("Transaction: ${event.transferId}. Outcome: $transactionOutcome")
            }
        }
    }
}