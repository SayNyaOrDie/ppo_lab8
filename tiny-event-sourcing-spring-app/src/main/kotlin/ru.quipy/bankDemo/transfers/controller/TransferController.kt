package ru.quipy.bankDemo.transfers.controller

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import ru.quipy.bankDemo.transfers.projections.TransferExistenceCache
import java.util.UUID

@RestController
@RequestMapping("/transfers")
class TransferController(
    val transferService: TransferExistenceCache
) {

    @GetMapping("/{transferId}/status")
    fun getTransactionStatus(@PathVariable transferId: UUID): String? {
        val transaction = transferService.findById(transferId)

        return transaction?.transferStatus
    }
}