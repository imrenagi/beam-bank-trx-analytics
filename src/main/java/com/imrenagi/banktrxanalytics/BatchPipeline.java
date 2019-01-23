package com.imrenagi.banktrxanalytics;

import com.imrenagi.banktrxanalytics.events.TransferCreated;
import com.imrenagi.banktrxanalytics.transforms.CalculatePeriodicTransfer;
import com.imrenagi.banktrxanalytics.transforms.CalculateTransfer;
import com.imrenagi.banktrxanalytics.transforms.StringToTransferCreated;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class BatchPipeline {

	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);

		PCollection<TransferCreated> transfers = p.apply(TextIO.read().from("transfer.txt"))
				.apply(ParDo.of(new StringToTransferCreated()))
				.apply(WithTimestamps.of((TransferCreated s) -> new Instant(s.getTimestamp())));

		/*
		TIL: In bounded dataset, triggering doesnt work like the way it works in unbounded dataset. The trigger is as if being
		ignored. So only the window works.
		 */
		PCollection<KV<String, Long>> globalWindow = transfers.apply(new CalculateTransfer(Duration.standardHours(1), Duration.standardMinutes(10)));

		globalWindow.apply(ParDo.of(new DoFn<KV<String, Long>, String>() {
			@ProcessElement
			public void processElement(ProcessContext context) {
				KV<String, Long> kv = context.element();
				String out = String.format("%s:\t%d", kv.getKey(), kv.getValue());
				context.output(out);
			}
		})).apply(TextIO.write().to("output-global"));

		transfers.apply(new CalculatePeriodicTransfer(Duration.standardMinutes(10),
				Duration.standardDays(1),
				Duration.standardMinutes(10),
				Duration.standardMinutes(5)))
				.apply(ParDo.of(new DoFn<KV<String, Long>, String>() {
					@ProcessElement
					public void processElement(IntervalWindow window, ProcessContext context) {

						KV<String, Long> kv = context.element();
						String out = String.format("%s - %s -> %s:\t%d", window.start(), window.end(), kv.getKey(), kv.getValue());
						context.output(out);
					}
				})).apply(TextIO.write().to("output-periodic"));

		p.run();
	}
}
