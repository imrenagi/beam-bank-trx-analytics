package com.imrenagi.banktrxanalytics.transforms;

import com.imrenagi.banktrxanalytics.events.TransferCreated;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

//Globally Calculate Total Transfer Created. For every periodTrigger after the first element arrive,
//accumulates all data in panes and calculate the total value for every given account.
public class CalculateTransfer extends PTransform<PCollection<TransferCreated>, PCollection<KV<String, Long>>> {

    private Duration allowedLateness;
    private Duration periodTrigger;

    public CalculateTransfer(Duration allowedLateness, Duration periodTrigger) {
        this.allowedLateness = allowedLateness;
        this.periodTrigger = periodTrigger;
    }

    @Override
    public PCollection<KV<String, Long>> expand(PCollection<TransferCreated> input) {
        return input
                .apply(Window.<TransferCreated>into(new GlobalWindows())
                        .triggering(Repeatedly.forever(
                                // after first element arrive in the global windows, wait 10 minutes, then trigger all data within that 10 minutes
                                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(periodTrigger)
                        ))
                        .accumulatingFiredPanes() // accumulating data
                        .withAllowedLateness(this.allowedLateness))
                .apply(new ExtractAndSumTransfer());
    }
}
