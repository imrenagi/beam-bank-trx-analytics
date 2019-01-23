package com.imrenagi.banktrxanalytics;

import com.imrenagi.banktrxanalytics.events.TransferCreated;
import com.imrenagi.banktrxanalytics.transforms.CalculatePeriodicTransfer;
import com.imrenagi.banktrxanalytics.transforms.PeriodicTransferToString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Date;

public class BatchPipeline {

	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);

		// Input
		PCollection<TransferCreated> transfers = p.apply(TextIO.read().from("transfer.txt"))
				.apply(ParDo.of(new StringToTransferCreated()))
				.apply(WithTimestamps.of((TransferCreated s) -> new Instant(s.getTimestamp())));

		/*
		TIL: In bounded dataset, triggering doesnt work like the way it works in unbounded dataset. The trigger is as if being
		ignored. So only the window works.
		 */
		transfers.apply(new CalculatePeriodicTransfer(Duration.standardMinutes(10), Duration.standardDays(1), Duration.standardMinutes(10), Duration.standardMinutes(5)))
				.apply(ParDo.of(new PeriodicTransferToString()))
				.apply(TextIO.write().to("output-periodic"));

		p.run();
	}

	private static class StringToTransferCreated extends DoFn<String, TransferCreated> {

		@ProcessElement
		public void processElement(ProcessContext context) {
			String element = context.element();
			String[] arr = element.split(",");

			DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
			DateTime dt = formatter.parseDateTime(arr[3]);

			context.output(
					new TransferCreated(
							arr[0],
							arr[1],
							Long.parseLong(arr[2]),
							dt.getMillis()));
		}
	}
}


//		PCollection<KV<String, Long>> globalWindow = transfers.apply(new CalculateTransfer(Duration.standardHours(1), Duration.standardMinutes(10)));
//		globalWindow.apply(ParDo.of(new DoFn<KV<String, Long>, String>() {
//			@ProcessElement
//			public void processElement(ProcessContext context) {
//				KV<String, Long> kv = context.element();
//				String out = String.format("%s:\t%d", kv.getKey(), kv.getValue());
//				context.output(out);
//			}
//		})).apply(TextIO.write().to("output-global"));