package com.imrenagi.banktrxanalytics.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TransferCreated {

    private String fromAccountID;
    private String toAccountID;
    private Long amount;
    private Long timestamp;

    public TransferCreated() {
    }

    public TransferCreated(String fromAccountID, String toAccountID, Long amount, Long timestamp) {
        this.fromAccountID = fromAccountID;
        this.toAccountID = toAccountID;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    public String getFromAccountID() {
        return fromAccountID;
    }

    public void setFromAccountID(String fromAccountID) {
        this.fromAccountID = fromAccountID;
    }

    public String getToAccountID() {
        return toAccountID;
    }

    public void setToAccountID(String toAccountID) {
        this.toAccountID = toAccountID;
    }

    public Long getAmount() {
        return amount;
    }

    public void setAmount(Long amount) {
        this.amount = amount;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
