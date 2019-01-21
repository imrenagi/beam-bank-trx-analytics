package com.imrenagi.banktrxanalytics.transforms;

import com.imrenagi.banktrxanalytics.events.TransferCreated;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

public class CalculateTransferTest {

	private static final Duration ALLOWED_LATENESS = Duration.standardHours(1);
	private static final Duration PERIOD_TRIGGER = Duration.standardMinutes(10);

	@Rule
	public TestPipeline p = TestPipeline.create();
	private Instant baseTime = new Instant(0);

	@Test
	public void CalculateTransfer_ShouldBeTriggeredOnceAfterOneAdvanceProcessingTime() {

		TestStream<TransferCreated> stream =
				TestStream.create(AvroCoder.of(TransferCreated.class))
						.advanceWatermarkTo(baseTime)
						.addElements(event("john", "elisa", 1000L, Duration.standardMinutes(1)))
						.addElements(event("john", "elisa", 1200L, Duration.standardMinutes(2)))
						.addElements(event("john", "elisa", 400L, Duration.standardMinutes(3)))
						.addElements(event("doe", "elisa", 700L, Duration.standardMinutes(5)))
						.advanceProcessingTime(Duration.standardMinutes(12))
						.advanceWatermarkToInfinity();

		PCollection<KV<String, Long>> transferSum =
				p.apply(stream).apply(new CalculateTransfer(ALLOWED_LATENESS, PERIOD_TRIGGER));

		PAssert
				.that(transferSum)
				.inEarlyGlobalWindowPanes()
				.containsInAnyOrder(
						KV.of("john", 2600L),
						KV.of("doe", 700L));

		p.run().waitUntilFinish();
	}

	@Test
	public void CalculateTransfeer_ShouldBeTriggeredTwiceAfterTwoAdvanceProcessingTime() {

		TestStream<TransferCreated> stream =
				TestStream.create(AvroCoder.of(TransferCreated.class))
						.advanceWatermarkTo(baseTime)
						.addElements(event("john", "elisa", 1000L, Duration.standardMinutes(1)))
						.addElements(event("john", "elisa", 1200L, Duration.standardMinutes(2)))
						.addElements(event("john", "elisa", 400L, Duration.standardMinutes(3)))
						.addElements(event("doe", "elisa", 700L, Duration.standardMinutes(5)))
						.advanceProcessingTime(Duration.standardMinutes(11))
						.addElements(event("john", "elisa", 1000L, Duration.standardMinutes(3)))
						.addElements(event("lexa", "elisa", 500L, Duration.standardMinutes(15)))
						//advanceProcessingTime advancing n new minutes, not from the previous advance.
						//advanceProcessingTime advancing from 0 again
						.advanceProcessingTime(Duration.standardMinutes(11))
						.advanceWatermarkToInfinity();

		PCollection<KV<String, Long>> transferSum =
				p.apply(stream).apply(new CalculateTransfer(ALLOWED_LATENESS, PERIOD_TRIGGER));

		PAssert
				.that(transferSum)
				.inEarlyGlobalWindowPanes()
				.containsInAnyOrder(
						KV.of("john", 3600L), //2nd trigger
						KV.of("john", 2600L), //1st trigger
						KV.of("doe", 700L), //1st trigger
						KV.of("lexa", 500L)); //2nd trigger

		p.run().waitUntilFinish();
	}

	private TimestampedValue<TransferCreated> event(
			String fromAccountID, String toAccountID, Long amount, Duration baseTimeOffset) {
		return TimestampedValue.of(
				new TransferCreated(fromAccountID, toAccountID, amount, baseTime.plus(baseTimeOffset).getMillis()),
				baseTime.plus(baseTimeOffset));
	}


}