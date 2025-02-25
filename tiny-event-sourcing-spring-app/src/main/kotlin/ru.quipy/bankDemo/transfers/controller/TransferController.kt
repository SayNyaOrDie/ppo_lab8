package ru.quipy.bankDemo.transfers.controller

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import ru.quipy.bankDemo.transfers.api.TransferTransactionAggregate
import ru.quipy.bankDemo.transfers.logic.TransferTransaction
import ru.quipy.core.EventSourcingService
import java.util.UUID

@RestController
@RequestMapping("/transfers")
class TransferController(
    val transferEsService: EventSourcingService<UUID, TransferTransactionAggregate, TransferTransaction>
) {

    @GetMapping("/{transferId}/status")
    fun getTransactionStatus(@PathVariable transferId: UUID): String {
        val transaction = transferEsService.getState(transferId)

        return transaction!!.transactionState.toString()
    }
}