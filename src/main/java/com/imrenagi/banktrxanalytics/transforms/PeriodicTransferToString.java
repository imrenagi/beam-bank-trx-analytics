package com.imrenagi.banktrxanalytics.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class PeriodicTransferToString extends DoFn<KV<String, Long>, String> {

	DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss");

	@ProcessElement
	public void processElement(IntervalWindow window, ProcessContext context) {

		KV<String, Long> kv = context.element();

		DateTime startDT = window.start().toDateTime();
		DateTime endDT = window.end().toDateTime();

		String out = String.format("%s - %s -> %s:\t%d", formatter.print(startDT), formatter.print(endDT), kv.getKey(), kv.getValue());
		context.output(out);
	}
}

