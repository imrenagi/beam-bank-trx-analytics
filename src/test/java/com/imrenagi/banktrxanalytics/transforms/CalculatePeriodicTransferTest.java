package com.imrenagi.banktrxanalytics.transforms;

import com.imrenagi.banktrxanalytics.events.TransferCreated;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.*;

public class CalculatePeriodicTransferTest {

	private static final Duration ALLOWED_LATENESS = Duration.standardHours(1);
	private static final Duration WINDOW_DURATION = Duration.standardMinutes(20);
	private static final Duration EARLY_TRIGGER_DELAY = Duration.standardMinutes(10);
	private static final Duration LATE_TRIGGER_DELAY = Duration.standardMinutes(5);

	@Rule
	public TestPipeline p = TestPipeline.create();
	private Instant baseTime = new Instant(0);

	@Test
	//test on time
	public void CalculatePeriodicTransfer_ShouldEmitOnceInTheEnd() {

		TestStream<TransferCreated> stream =
				TestStream.<TransferCreated>create(AvroCoder.of(TransferCreated.class))
						.advanceWatermarkTo(baseTime)
						.addElements(event("john", "elisa", 1000L, Duration.standardMinutes(1)))
						.addElements(event("john", "lisa", 1000L, Duration.standardMinutes(2)))
						.addElements(event("john", "graham", 1200L, Duration.standardMinutes(13)))
						.addElements(event("john", "joy", 100L, Duration.standardMinutes(15)))
						.advanceWatermarkToInfinity();

		PCollection<KV<String, Long>> transferSum =
				p.apply(stream)
						.apply(new CalculatePeriodicTransfer(WINDOW_DURATION, ALLOWED_LATENESS, EARLY_TRIGGER_DELAY, LATE_TRIGGER_DELAY));

		PAssert
				.that(transferSum)
				.inOnTimePane(new IntervalWindow(baseTime, WINDOW_DURATION))
				.containsInAnyOrder(
						KV.of("john", 3300L));

		p.run().waitUntilFinish();
	}

	@Test
	// test on time but almost closes the window
	public void CalculatePeriodicTransfer_ShouldEmitOnceInTheEndWithWatermarkSlightlyAdvance() {

		TestStream<TransferCreated> stream =
				TestStream.<TransferCreated>create(AvroCoder.of(TransferCreated.class))
						.advanceWatermarkTo(baseTime)
						.addElements(event("john", "elisa", 1000L, Duration.standardMinutes(1)))
						.addElements(event("john", "lisa", 1000L, Duration.standardMinutes(2)))
						// The watermark advances slightly, but not past the end of the window
						.advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(3)))
						.addElements(event("john", "graham", 1200L, Duration.standardMinutes(13)))
						.addElements(event("john", "joy", 100L, Duration.standardMinutes(15)))
						.advanceWatermarkToInfinity();

		PCollection<KV<String, Long>> transferSum =
				p.apply(stream)
						.apply(new CalculatePeriodicTransfer(WINDOW_DURATION, ALLOWED_LATENESS, EARLY_TRIGGER_DELAY, LATE_TRIGGER_DELAY));

		PAssert
				.that(transferSum)
				.inOnTimePane(new IntervalWindow(baseTime, WINDOW_DURATION))
				.containsInAnyOrder(
						KV.of("john", 3300L));

		p.run().waitUntilFinish();
	}


	/**
	 * A test of the CalculatePeriodicTransfer when all of the elements arrive on
	 * time, and the processing time advances far enough for speculative panes.
	 */
	@Test
	public void CalculatePeriodicTransfer_SpeculativeInTwoWindows() {

		TestStream<TransferCreated> stream =
				TestStream.<TransferCreated>create(AvroCoder.of(TransferCreated.class))
						.advanceWatermarkTo(baseTime)
						//first windo
						.addElements(event("john", "elisa", 1000L, Duration.standardMinutes(1)))
						.addElements(event("john", "elisa", 500L, Duration.standardMinutes(2)))
						.addElements(event("tiffani", "elisa", 100L, Duration.standardMinutes(11)))
						.addElements(event("tiffani", "elisa", 50L, Duration.standardMinutes(11)))
						// More time passes and a speculative pane containing a refined value for the blue pane is emitted
						.advanceProcessingTime(Duration.standardMinutes(11))
						.addElements(event("john", "elisa", 1000L, Duration.standardMinutes(11)))
						.addElements(event("alex", "john", 250L, Duration.standardMinutes(12)))
						.advanceProcessingTime(Duration.standardMinutes(12))
						//second window
						.addElements(event("john", "elisa", 1000L, Duration.standardMinutes(25)))
						.addElements(event("john", "elisa", 800L, Duration.standardMinutes(26)))
						// More time passes and a speculative pane containing a refined value for the blue pane is emitted
						.advanceProcessingTime(Duration.standardMinutes(12))
						.addElements(event("john", "elisa", 50L, Duration.standardMinutes(27)))
						.addElements(event("alex", "tiffani", 1000L, Duration.standardMinutes(29)))
						.advanceWatermarkToInfinity();

		PCollection<KV<String, Long>> transferSum =
				p.apply(stream)
						.apply(new CalculatePeriodicTransfer(WINDOW_DURATION, ALLOWED_LATENESS,
								EARLY_TRIGGER_DELAY, LATE_TRIGGER_DELAY));

		// first window
		IntervalWindow window = new IntervalWindow(baseTime, WINDOW_DURATION);
		PAssert.that(transferSum)
				.inOnTimePane(window)
				.containsInAnyOrder(
						KV.of("john", 2500L),
						KV.of("tiffani",150L),
						KV.of("alex", 250L));

		PAssert
				.that(transferSum)
				.inWindow(window)
				.satisfies(input -> {
					Assert.assertThat(input, hasItem(KV.of("john", 1500L))); //1st speculative pane
					Assert.assertThat(input, hasItem(KV.of("tiffani", 150L))); //1nd speculative pane
					Assert.assertThat(input, hasItem(KV.of("john", 2500L))); //2nd speculative pane
					Assert.assertThat(input, hasItem(KV.of("alex", 250L))); //2st speculative pane
					return null;
				});

		//the next 20 minutes
		IntervalWindow secondWindow = new IntervalWindow(new Instant(20 * 60 * 1000), Duration.standardMinutes(20));
		PAssert
				.that(transferSum)
				.inWindow(secondWindow)
				.satisfies(input -> {
					Assert.assertThat(input, hasItem(KV.of("john", 1800L)));
					Assert.assertThat(input, hasItem(KV.of("john", 1850L)));
					Assert.assertThat(input, hasItem(KV.of("alex", 1000L)));
					return null;
				});

		PAssert
				.that(transferSum)
				.inOnTimePane(secondWindow)
				.containsInAnyOrder(
						KV.of("john", 1850L),
						KV.of("alex", 1000L));

		PAssert
				.that(transferSum)
				.inFinalPane(secondWindow)
				.satisfies(input -> {
					Assert.assertThat(input, hasItem(KV.of("john", 1850L)));
					Assert.assertThat(input, hasItem(KV.of("alex", 1000L)));
					return null;
				});

		p.run().waitUntilFinish();
	}

	/**
	 * A test where elements arrive behind the watermark (late data), but before the end of the
	 * window. These elements are emitted on time.
	 */
	@Test
	public void CalculatePeriodicTransfer_UnObservablyLate() {

		TestStream<TransferCreated> stream =
				TestStream.<TransferCreated>create(AvroCoder.of(TransferCreated.class))
						.advanceWatermarkTo(baseTime)
						.addElements(event("john", "elisa", 1000L, Duration.standardMinutes(1)))
						.addElements(event("john", "elisa", 200L, Duration.standardMinutes(2)))
						.addElements(event("elisa", "john", 10L, Duration.standardMinutes(11)))
						.addElements(event("elisa", "elisa", 70L, Duration.standardMinutes(11)))
						.advanceWatermarkTo(
								baseTime.plus(WINDOW_DURATION).minus(Duration.standardMinutes(1)))
						// These events are late, but the window hasn't closed yet, so the elements are in the
						// on-time pane
						.addElements(event("elisa", "tiffani", 870L, Duration.standardMinutes(11)))
						.addElements(event("john", "alex", 1000L, Duration.standardMinutes(12)))
						.advanceWatermarkTo(
								baseTime.plus(WINDOW_DURATION).minus(Duration.standardMinutes(1)))
						.advanceWatermarkToInfinity();

		PCollection<KV<String, Long>> transferSum =
				p.apply(stream)
						.apply(new CalculatePeriodicTransfer(WINDOW_DURATION, ALLOWED_LATENESS,
								EARLY_TRIGGER_DELAY, LATE_TRIGGER_DELAY));

		// first window
		IntervalWindow window = new IntervalWindow(baseTime, WINDOW_DURATION);
		PAssert
				.that(transferSum)
				.inOnTimePane(window)
				.containsInAnyOrder(
						KV.of("john", 2200L),
						KV.of("elisa",950L)
				);

		PAssert
				.that(transferSum)
				.inWindow(window)
				.satisfies(input -> {
					//There is no speculative since it is advanced by the watermark, not processing time
					Assert.assertThat(input, hasItem(KV.of("john", 2200L)));
					Assert.assertThat(input, hasItem(KV.of("elisa", 950L)));
					return null;
				});

		p.run().waitUntilFinish();
	}

	/**
	 * A test where elements arrive behind the watermark (late data) after the watermark passes the
	 * end of the window, but before the maximum allowed lateness. These elements are emitted in a
	 * late pane.
	 */
	@Test
	public void CalculatePeriodicTransfer_ObservablyLate() {

		Instant firstWindowCloses = baseTime.plus(ALLOWED_LATENESS).plus(WINDOW_DURATION);

		TestStream<TransferCreated> stream =
				TestStream.<TransferCreated>create(AvroCoder.of(TransferCreated.class))
						.advanceWatermarkTo(baseTime)
						.addElements(event("john", "doe", 100L, Duration.standardMinutes(1)))
						.addElements(event("john", "doe", 100L, Duration.standardMinutes(2)))
						.addElements(event("john", "doe", 100L, Duration.standardMinutes(11)))
						.advanceProcessingTime(Duration.standardMinutes(10))
//            .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(3)))
						.advanceWatermarkTo(firstWindowCloses.minus(Duration.standardMinutes(1)))
						//These events are late, but should be observable in pane because not pass the allowedlateness
						.addElements(event("alexa", "doe", 160L, Duration.standardMinutes(3)))
						.addElements(event("john", "doe", 50L, Duration.standardMinutes(3)))
						.advanceProcessingTime(Duration.standardMinutes(6))
						.addElements(event("john", "doe", 100L, Duration.standardMinutes(3)))
						.addElements(event("john", "doe", 100L, Duration.standardMinutes(3)))
						.addElements(event("john", "doe", 100L, Duration.standardMinutes(3)))
						.addElements(event("john", "doe", 100L, Duration.standardMinutes(3)))

						//if the processingTime advances, then the 750L will not be emitted in the final pane
						//because it has been emitted in the second late speculative pane
//						.advanceProcessingTime(Duration.standardMinutes(6))

						.advanceWatermarkToInfinity();

		PCollection<KV<String, Long>> transferSum =
				p.apply(stream)
						.apply(new CalculatePeriodicTransfer(WINDOW_DURATION, ALLOWED_LATENESS,
								EARLY_TRIGGER_DELAY, LATE_TRIGGER_DELAY));

		// first window
		IntervalWindow window = new IntervalWindow(baseTime, WINDOW_DURATION);
		PAssert.that(transferSum)
				.inOnTimePane(window)
				.containsInAnyOrder(
						KV.of("john", 300L)
				);

		PAssert.that(transferSum)
				.inWindow(window)
				.containsInAnyOrder(
						KV.of("john", 300L), //first speculative
						KV.of("alexa", 160L), // first late speculative
						KV.of("john", 350L), // first late speculative
//						KV.of("john", 400L) // second late speculative. this only happened if the processing time advances to the second late trigger
						KV.of("john", 750L) //final pane
				);

		PAssert
				.that(transferSum)
				.inFinalPane(window)
				.satisfies(input -> {
					Assert.assertThat(input, hasItem(KV.of("john", 750L)));

					//has been emmitted in earlier pane, so it should not appear on the final pane
					Assert.assertThat(input, not(hasItem(KV.of("alexa", 160L))));
					return null;
				});

		p.run().waitUntilFinish();
	}

	/**
	 * A test where elements arrive beyond the maximum allowed lateness. These elements are dropped
	 * within CalculatePeriodicTransfer and do not impact the final result.
	 */
	@Test
	public void testTeamScoresDroppablyLate() {

		BoundedWindow window = new IntervalWindow(baseTime, WINDOW_DURATION);

		TestStream<TransferCreated> stream =
				TestStream.create(AvroCoder.of(TransferCreated.class))
						.addElements(
								event("john", "doe", 100L, Duration.standardMinutes(1)),
								event("alexa", "doe", 50L, Duration.standardMinutes(1)))
						.advanceWatermarkTo(window.maxTimestamp())
						.addElements(
								event("john", "doe", 10L, Duration.standardMinutes(2)),
								event("alexa", "doe", 30L, Duration.standardMinutes(6)))
						// Move the watermark to the end of the window to output on time
						.advanceWatermarkTo(baseTime.plus(WINDOW_DURATION))
						// Move the watermark past the end of the allowed lateness plus the end of the window
						.advanceWatermarkTo(
								baseTime
										.plus(ALLOWED_LATENESS)
										.plus(WINDOW_DURATION)
										.plus(Duration.standardMinutes(1)))
						// These elements within the expired window are droppably late, and will not appear in
						// the output
						.addElements(
								event("john", "doe", 100L, Duration.standardMinutes(1)),
								event("tiffani", "doe", 200L, Duration.standardMinutes(1)))
						.advanceWatermarkToInfinity();

		PCollection<KV<String, Long>> transferSum =
				p.apply(stream)
						.apply(new CalculatePeriodicTransfer(WINDOW_DURATION, ALLOWED_LATENESS,
								EARLY_TRIGGER_DELAY, LATE_TRIGGER_DELAY));


		// Only one on-time pane and no late panes should be emitted
		PAssert.that(transferSum)
				.inWindow(window)
				.containsInAnyOrder(KV.of("john", 110L), KV.of("alexa", 80L));

		// No elements are added before the watermark passes the end of the window plus the allowed
		// lateness, so no refinement should be emitted
		// Notes: refinement will always be in final pane
		PAssert.that(transferSum).inFinalPane(window).empty();

		p.run().waitUntilFinish();
	}


		private TimestampedValue<TransferCreated> event(
			String fromAccountID, String toAccountID, Long amount, Duration baseTimeOffset) {
		return TimestampedValue.of(
				new TransferCreated(fromAccountID, toAccountID, amount, baseTime.plus(baseTimeOffset).getMillis()),
				baseTime.plus(baseTimeOffset));
	}

}