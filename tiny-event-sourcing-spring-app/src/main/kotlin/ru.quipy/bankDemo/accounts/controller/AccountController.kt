package ru.quipy.bankDemo.accounts.controller

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import ru.quipy.bankDemo.accounts.api.AccountAggregate
import ru.quipy.bankDemo.accounts.api.AccountCreatedEvent
import ru.quipy.bankDemo.accounts.api.BankAccountCreatedEvent
import ru.quipy.bankDemo.accounts.api.BankAccountDepositEvent
import ru.quipy.bankDemo.accounts.logic.Account
import ru.quipy.bankDemo.accounts.logic.BankAccount
import ru.quipy.bankDemo.transfers.api.TransferTransactionAggregate
import ru.quipy.bankDemo.transfers.api.TransferTransactionCreatedEvent
import ru.quipy.bankDemo.transfers.logic.TransferTransaction
import ru.quipy.core.EventSourcingService
import ru.quipy.saga.SagaManager
import java.math.BigDecimal
import java.util.*

@RestController
@RequestMapping("/accounts")
class AccountController(
    val accountEsService: EventSourcingService<UUID, AccountAggregate, Account>,
    val transferEsService: EventSourcingService<UUID, TransferTransactionAggregate, TransferTransaction>,
    val sagaManager: SagaManager
) {
    val transferSagaName = "TRANSFER_PROCESSING"


//    holderId != accountId
//    holder - пользователь системы
//    account - аккаунт holder'а, может быть сколько угодно
//    bankAccount - банковский счёт, привязанный к account (макс 5)
    @PostMapping("/{holderId}")
    fun createAccount(@PathVariable holderId: UUID) : AccountCreatedEvent {
        return accountEsService.create { it.createNewAccount(holderId = holderId) }
    }

    @GetMapping("/{accountId}")
    fun getAccount(@PathVariable accountId: UUID) : Account? {
        return accountEsService.getState(accountId)
    }

    @PostMapping("/{accountId}/bankAccount")
    fun createBankAccount(@PathVariable accountId: UUID) : BankAccountCreatedEvent {
        return accountEsService.update(accountId) { it.createNewBankAccount() }
    }

    @GetMapping("/{accountId}/bankAccount/{bankAccountId}")
    fun getBankAccount(@PathVariable accountId: UUID, @PathVariable bankAccountId: UUID) : BankAccount? {
        return accountEsService.getState(accountId)?.bankAccounts?.get(bankAccountId)
    }

    @PostMapping("/{bankAccountId}/deposit")
    fun deposit(@PathVariable bankAccountId: UUID, @RequestParam amount: BigDecimal): BankAccountDepositEvent {
        return accountEsService.create { it.deposit(bankAccountId, amount) }
    }

//    Перевод между аккаунтами двух РАЗНЫХ пользователей
    @PostMapping("/{sourceAccountId}/bankAccount/{sourceBankAccountId}/transfer")
    fun transferToAccount(
    @PathVariable sourceAccountId: UUID,
    @PathVariable sourceBankAccountId: UUID,
    @RequestParam destinationAccountId: UUID,
    @RequestParam destinationBankAccountId: UUID,
    @RequestParam transferAmount: BigDecimal
    ): TransferTransactionCreatedEvent {
        val sagaContext = sagaManager
            .launchSaga(transferSagaName, "start processing")
            .sagaContext()
//    в примере sagaContext без (), но у меня ругается
        return transferEsService.create(sagaContext) {
            it.initiateTransferTransaction(
                sagaContext.ctx[transferSagaName]!!.sagaInstanceId,
                sourceAccountId,
                sourceBankAccountId,
                destinationAccountId,
                destinationBankAccountId,
                transferAmount
            )
        }
    }
}