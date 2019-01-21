package com.imrenagi.banktrxanalytics.transforms;

import com.imrenagi.banktrxanalytics.events.TransferCreated;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;


// Calculate Total Transfer Created for every windowDuration given. For every window, the early trigger fires every earlyTriggerDuration
// after the first element arrived on that pane. Once the window ends, the late trigger fires every lateTriggerDuration
// after the first element arrived on that pane. All elements arrive after allowedLatenessDuration will be discarded.
// Every time trigger fires, it accumulates all data on the respective pane.
public class CalculatePeriodicTransfer extends PTransform<PCollection<TransferCreated>, PCollection<KV<String, Long>>> {

	private Duration windowDuration;
	private Duration allowedLatenessDuration;
	private Duration earlyTriggerDuration;
	private Duration lateTriggerDuration;

	public CalculatePeriodicTransfer(Duration windowDuration,
	                                 Duration allowedLatenessDuration,
	                                 Duration earlyTriggerDuration,
	                                 Duration lateTriggerDuration) {
		this.windowDuration = windowDuration;
		this.allowedLatenessDuration = allowedLatenessDuration;
		this.earlyTriggerDuration = earlyTriggerDuration;
		this.lateTriggerDuration = lateTriggerDuration;
	}

	@Override
	public PCollection<KV<String, Long>> expand(PCollection<TransferCreated> input) {
		return input
				.apply(Window.<TransferCreated>into(FixedWindows.of(this.windowDuration))
						//set triggering
						.triggering(
								AfterWatermark.pastEndOfWindow()
										.withEarlyFirings(
												//early firing every 10 minutes after processing time of the first element
												// within that window.
												AfterProcessingTime.pastFirstElementInPane()
														.plusDelayOf(this.earlyTriggerDuration))
										.withLateFirings(
												//fires every 5 minutes until allowed lateness minutes
												AfterProcessingTime.pastFirstElementInPane()
														.plusDelayOf(this.lateTriggerDuration)
										)
						).withAllowedLateness(this.allowedLatenessDuration)
						.accumulatingFiredPanes() //accumulating all data
				).apply(new ExtractTransfer());
	}
}