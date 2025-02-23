package ru.quipy.bankDemo.accounts.logic

import ru.quipy.bankDemo.accounts.api.*
import ru.quipy.bankDemo.transfers.api.TransferParticipantRollbackedEvent
import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.domain.AggregateState
import ru.quipy.domain.Event
import java.math.BigDecimal
import java.util.*
import kotlin.reflect.typeOf

// вопрос, что делать, если, скажем, обрабатываем какой-то ивент, понимаем, что агрегата, который нужно обновить не существует.
// Может ли ивент (ошибка) существовать в отрыве от агрегата?


class Account : AggregateState<UUID, AccountAggregate> {
    lateinit var accountId: UUID
    lateinit var holderId: UUID
    var bankAccounts: MutableMap<UUID, BankAccount> = mutableMapOf()

    override fun getId() = accountId

    fun createNewAccount(id: UUID = UUID.randomUUID(), holderId: UUID): AccountCreatedEvent {
        return AccountCreatedEvent(id, holderId)
    }

    fun createNewBankAccount(): BankAccountCreatedEvent {
        if (bankAccounts.size >= 5)
            throw IllegalStateException("Account $accountId already has ${bankAccounts.size} bank accounts")

        return BankAccountCreatedEvent(accountId = accountId, bankAccountId = UUID.randomUUID())
    }

    fun deposit(toBankAccountId: UUID, amount: BigDecimal): BankAccountDepositEvent {
        val bankAccount = (bankAccounts[toBankAccountId]
            ?: throw IllegalArgumentException("No such account to transfer to: $toBankAccountId, ${bankAccounts[toBankAccountId]}"))

        if (bankAccount.balance + amount > BigDecimal(10_000_000))
            throw IllegalStateException("You can't store more than 10.000.000 on account ${bankAccount.id}")

        if (bankAccounts.values.sumOf { it.balance } + amount > BigDecimal(25_000_000))
            throw IllegalStateException("You can't store more than 25.000.000 in total")


        return BankAccountDepositEvent(
            accountId = accountId,
            bankAccountId = toBankAccountId,
            amount = amount
        )
    }

    fun performTransferTo(
        bankAccountId: UUID,
        transactionId: UUID,
        transferAmount: BigDecimal
    ): Event<AccountAggregate> {
        val bankAccount = bankAccounts[bankAccountId]
            ?: throw IllegalArgumentException("No such account to transfer to: $bankAccountId")

//        заменить на rollback
        if (bankAccount.balance + transferAmount > BigDecimal(10_000_000)) {
            return TransferTransactionRollbackedEvent(
                accountId = accountId,
                bankAccountId = bankAccountId,
                transactionId = transactionId,
                "User can't store more than 10.000.000 on account: ${bankAccount.id}"
            )
        }

        if (bankAccounts.values.sumOf { it.balance } + transferAmount > BigDecimal(25_000_000)) {
            return TransferTransactionRollbackedEvent(
                accountId = accountId,
                bankAccountId = bankAccountId,
                transactionId = transactionId,
                "User can't store more than 25.000.000 in total on account: ${bankAccount.id}"
            )
        }

        return TransferTransactionProcessedEvent(
            accountId = accountId,
            bankAccountId = bankAccountId,
            transactionId = transactionId,
            transferAmount = transferAmount,
            isDeposit = true
        )
    }

    fun withdraw(fromBankAccountId: UUID, amount: BigDecimal): BankAccountWithdrawalEvent {
        val fromBankAccount = bankAccounts[fromBankAccountId]
            ?: throw IllegalArgumentException("No such account to withdraw from: $fromBankAccountId")

        if (amount > fromBankAccount.balance) {
            throw IllegalArgumentException("Cannot withdraw $amount. Not enough money: ${fromBankAccount.balance}")
        }

        return BankAccountWithdrawalEvent(
            accountId = accountId,
            bankAccountId = fromBankAccountId,
            amount = amount
        )
    }

    fun performTransferFrom(
        bankAccountId: UUID,
        transactionId: UUID,
        transferAmount: BigDecimal
    ): Event<AccountAggregate> {
        val bankAccount = bankAccounts[bankAccountId]
            ?: throw IllegalArgumentException("No such account to transfer from: $bankAccountId")

        if (transferAmount > bankAccount.balance) {
            return TransferTransactionDeclinedEvent(
                accountId = accountId,
                bankAccountId = bankAccountId,
                transactionId = transactionId,
                "Cannot withdraw $transferAmount. Not enough money: ${bankAccount.balance}"
            )
        }

        return TransferTransactionAcceptedEvent(
            accountId = accountId,
            bankAccountId = bankAccountId,
            transactionId = transactionId,
            transferAmount = transferAmount,
            isDeposit = false
        )
    }

//    fun processPendingTransaction(
//        bankAccountId: UUID,
//        transactionId: UUID,
//    ): TransferTransactionProcessedEvent {
//        val pendingTransaction = bankAccounts[bankAccountId]!!.pendingTransactions[transactionId]!!
//        // todo sukhoa validation
//        return TransferTransactionProcessedEvent(
//            this.accountId,
//            bankAccountId,
//            transactionId
//        )
//    }

    fun rollbackTransaction(
        bankAccountId: UUID,
        transactionId: UUID,
        transferAmount: BigDecimal
    ): TransferTransactionCanceledEvent {
//        val pendingTransaction = bankAccounts[bankAccountId]!!.pendingTransactions[transactionId]!!
        // todo sukhoa validation
        return TransferTransactionCanceledEvent(
            this.accountId,
            bankAccountId,
            transactionId,
            transferAmount
        )
    }

    fun transferBetweenInternalAccounts(
        fromBankAccountId: UUID,
        toBankAccountId: UUID,
        transferAmount: BigDecimal
    ): InternalAccountTransferEvent {
        val bankAccountFrom = bankAccounts[fromBankAccountId]
            ?: throw IllegalArgumentException("No such account to withdraw from: $fromBankAccountId")

        if (transferAmount > bankAccountFrom.balance) {
            throw IllegalArgumentException("Cannot withdraw $transferAmount. Not enough money: ${bankAccountFrom.balance}")
        }

        val bankAccountTo = (bankAccounts[toBankAccountId]
            ?: throw IllegalArgumentException("No such account to transfer to: $toBankAccountId"))


        if (bankAccountTo.balance + transferAmount > BigDecimal(10_000_000))
            throw IllegalStateException("You can't store more than 10.000.000 on account ${bankAccountTo.id}")

        return InternalAccountTransferEvent(
            accountId = accountId,
            bankAccountIdFrom = fromBankAccountId,
            bankAccountIdTo = toBankAccountId,
            amount = transferAmount
        )
    }

    @StateTransitionFunc
    fun createNewBankAccount(event: AccountCreatedEvent) {
        accountId = event.accountId
        holderId = event.userId
    }

    @StateTransitionFunc
    fun createNewBankAccount(event: BankAccountCreatedEvent) {
        bankAccounts[event.bankAccountId] = BankAccount(event.bankAccountId)
    }

    @StateTransitionFunc
    fun deposit(event: BankAccountDepositEvent) {
        bankAccounts[event.bankAccountId]!!.deposit(event.amount)
    }

    @StateTransitionFunc
    fun withdraw(event: BankAccountWithdrawalEvent) {
        bankAccounts[event.bankAccountId]!!.withdraw(event.amount)
    }

    @StateTransitionFunc
    fun internalAccountTransfer(event: InternalAccountTransferEvent) {
        bankAccounts[event.bankAccountIdFrom]!!.withdraw(event.amount)
        bankAccounts[event.bankAccountIdTo]!!.deposit(event.amount)
    }

    @StateTransitionFunc
    fun acceptTransfer(event: TransferTransactionAcceptedEvent) {
        bankAccounts[event.bankAccountId]!!.withdraw(event.transferAmount)
    }

    @StateTransitionFunc
    fun processTransfer(event: TransferTransactionProcessedEvent) =
        bankAccounts[event.bankAccountId]!!.deposit(event.transferAmount)

    @StateTransitionFunc
    fun rollbackTransfer(event: TransferTransactionCanceledEvent) =
        bankAccounts[event.bankAccountId]!!.deposit(event.transferAmount)

    @StateTransitionFunc
    fun externalAccountTransferDecline(event: TransferTransactionDeclinedEvent) = Unit

    @StateTransitionFunc
    fun accountRollback(event: TransferTransactionRollbackedEvent) = Unit
}


data class BankAccount(
    val id: UUID,
    internal var balance: BigDecimal = BigDecimal.ZERO,
    internal var pendingTransactions: MutableMap<UUID, PendingTransaction> = mutableMapOf()
) {
    fun deposit(amount: BigDecimal) {
        this.balance = this.balance.add(amount)
    }

    fun withdraw(amount: BigDecimal) {
        this.balance = this.balance.subtract(amount)
    }

//    по идее вот это
    fun initiatePendingTransaction(pendingTransaction: PendingTransaction) {
        if (!pendingTransaction.isDeposit) {
            withdraw(pendingTransaction.transferAmountFrozen)
        }
        pendingTransactions[pendingTransaction.transactionId] = pendingTransaction
    }

    fun processPendingTransaction(trId: UUID) {
        val pendingTransaction = pendingTransactions.remove(trId)!!
        if (pendingTransaction.isDeposit) {
            deposit(pendingTransaction.transferAmountFrozen)
        }
    }

    fun rollbackPendingTransaction(trId: UUID) {
        val pendingTransaction = pendingTransactions.remove(trId)!!
        if (!pendingTransaction.isDeposit) {
            deposit(pendingTransaction.transferAmountFrozen) // refund
        }
    }
}

data class PendingTransaction(
    val transactionId: UUID,
    val transferAmountFrozen: BigDecimal,
    val isDeposit: Boolean
)