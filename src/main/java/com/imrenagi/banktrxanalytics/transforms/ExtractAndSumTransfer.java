package com.imrenagi.banktrxanalytics.transforms;

import com.imrenagi.banktrxanalytics.events.TransferCreated;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class ExtractAndSumTransfer extends PTransform<PCollection<TransferCreated>, PCollection<KV<String, Long>>> {

    @Override
    public PCollection<KV<String, Long>> expand(PCollection<TransferCreated> input) {
        return input.apply(
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                        .via((TransferCreated event) -> KV.of(event.getFromAccountID(), event.getAmount())))
                .apply(Sum.longsPerKey());
    }
}
