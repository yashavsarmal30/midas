package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionConsumer {
    private static final Logger logger = LoggerFactory.getLogger(TransactionConsumer.class);

    private final DatabaseConduit databaseConduit;

    public TransactionConsumer(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);

        UserRecord sender = databaseConduit.findUserById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findUserById(transaction.getRecipientId());

        if (sender != null && recipient != null) {
            if (sender.getBalance() >= transaction.getAmount()) {
                // Adjust balances
                sender.setBalance(sender.getBalance() - transaction.getAmount());
                recipient.setBalance(recipient.getBalance() + transaction.getAmount());

                // Save updated users and transaction record
                databaseConduit.save(sender);
                databaseConduit.save(recipient);
                databaseConduit.save(new TransactionRecord(sender, recipient, transaction.getAmount()));
                logger.info("Transaction processed successfully: {}", transaction);
            } else {
                logger.warn("Transaction failed validation (insufficient balance): {}", transaction);
            }
        } else {
            logger.warn("Transaction failed validation (invalid sender or recipient): {}", transaction);
        }
    }
}
