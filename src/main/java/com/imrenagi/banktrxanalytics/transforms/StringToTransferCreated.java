package com.imrenagi.banktrxanalytics.transforms;

import com.imrenagi.banktrxanalytics.events.TransferCreated;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.SimpleDateFormat;

public class StringToTransferCreated extends DoFn<String, TransferCreated> {

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

