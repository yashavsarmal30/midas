package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionConsumer {
    private static final Logger logger = LoggerFactory.getLogger(TransactionConsumer.class);

    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate;

    public TransactionConsumer(DatabaseConduit databaseConduit, RestTemplateBuilder restTemplateBuilder) {
        this.databaseConduit = databaseConduit;
        this.restTemplate = restTemplateBuilder.build();
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);

        UserRecord sender = databaseConduit.findUserById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findUserById(transaction.getRecipientId());

        if (sender != null && recipient != null) {
            if (sender.getBalance() >= transaction.getAmount()) {
                // Call incentives API
                float incentiveAmount = 0.0f;
                try {
                    Incentive incentive = restTemplate.postForObject("http://localhost:8080/incentive", transaction, Incentive.class);
                    if (incentive != null) {
                        incentiveAmount = incentive.getAmount();
                    }
                } catch (Exception e) {
                    logger.error("Failed to fetch incentive: ", e);
                }

                // Adjust balances
                sender.setBalance(sender.getBalance() - transaction.getAmount());
                recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

                // Save updated users and transaction record
                databaseConduit.save(sender);
                databaseConduit.save(recipient);
                databaseConduit.save(new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount));
                logger.info("Transaction processed successfully with incentive {}: {}", incentiveAmount, transaction);
            } else {
                logger.warn("Transaction failed validation (insufficient balance): {}", transaction);
            }
        } else {
            logger.warn("Transaction failed validation (invalid sender or recipient): {}", transaction);
        }
    }
}
