package com.imrenagi.banktrxanalytics;

import com.imrenagi.banktrxanalytics.events.TransferCreated;
import com.imrenagi.banktrxanalytics.transforms.CalculatePeriodicTransfer;
import com.imrenagi.banktrxanalytics.transforms.PeriodicTransferToString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.ReadableDuration;

import java.util.Random;

public class StreamingPipeline {

	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);

		// Input
		PCollection<TransferCreated> transfers = p.apply(GenerateSequence.from(0).withRate(10, Duration.standardSeconds(5)))
				.apply(ParDo.of(new LongToTransferCreated()))
				.apply(WithTimestamps.of((TransferCreated s) -> new Instant(s.getTimestamp())));

		transfers.apply(new CalculatePeriodicTransfer(Duration.standardSeconds(10), Duration.standardDays(1), Duration.standardSeconds(10), Duration.standardSeconds(5)))
				.apply(ParDo.of(new PeriodicTransferToString()))
				.apply(TextIO.write().to("stream-output-periodic").withWindowedWrites().withNumShards(1));

		p.run();
	}

	private static class LongToTransferCreated extends DoFn<Long, TransferCreated> {
		@ProcessElement
		public void processElement(ProcessContext context) {
			context.output(new TransferCreated("john", "elisa", 10L, System.currentTimeMillis()));
		}
	}
}
